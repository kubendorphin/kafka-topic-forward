package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// Forwarder 负责从源 topic 消费并转发到目标 topic
type Forwarder struct {
	cfg      *Config
	reader   *kafka.Reader
	writer   *kafka.Writer
	msgLim   *rate.Limiter // 按条数限速
	byteLim  *rate.Limiter // 按字节限速

	// 统计
	totalMsgs  uint64
	totalBytes uint64
}

// NewForwarder 根据配置构造转发器
func NewForwarder(cfg *Config) *Forwarder {
	startOffset := kafka.LastOffset
	if cfg.Forward.StartFromEarliest {
		startOffset = kafka.FirstOffset
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Source.Brokers,
		Topic:          cfg.Source.Topic,
		GroupID:        cfg.Source.GroupID,
		StartOffset:    startOffset,
		MinBytes:       cfg.Forward.MinBytes,
		MaxBytes:       cfg.Forward.MaxBytes,
		CommitInterval: cfg.Forward.CommitInterval,
	})

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Dest.Brokers...),
		Topic:        cfg.Dest.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    cfg.Forward.BatchSize,
		BatchTimeout: cfg.Forward.BatchTimeout,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	f := &Forwarder{
		cfg:    cfg,
		reader: reader,
		writer: writer,
	}

	if cfg.RateLimit.Enabled {
		if cfg.RateLimit.MessagesPerSecond > 0 {
			burst := cfg.RateLimit.Burst
			if burst <= 0 {
				burst = cfg.RateLimit.MessagesPerSecond
			}
			f.msgLim = rate.NewLimiter(rate.Limit(cfg.RateLimit.MessagesPerSecond), burst)
		}
		if cfg.RateLimit.BytesPerSecond > 0 {
			burst := cfg.RateLimit.Burst
			if burst <= 0 || burst < cfg.RateLimit.MaxBurstBytesHint() {
				burst = cfg.RateLimit.BytesPerSecond
			}
			f.byteLim = rate.NewLimiter(rate.Limit(cfg.RateLimit.BytesPerSecond), burst)
		}
	}

	return f
}

// MaxBurstBytesHint 字节令牌桶容量至少要能容纳一条消息, 这里给个经验下限
func (r RateLimitConfig) MaxBurstBytesHint() int {
	return 1 << 20 // 1MB
}

// Run 启动转发, 直到 ctx 取消
func (f *Forwarder) Run(ctx context.Context) error {
	defer f.close()

	log.Printf("开始转发: %v/%s -> %v/%s (group=%s, fromEarliest=%v)",
		f.cfg.Source.Brokers, f.cfg.Source.Topic,
		f.cfg.Dest.Brokers, f.cfg.Dest.Topic,
		f.cfg.Source.GroupID, f.cfg.Forward.StartFromEarliest)

	// 定期打印统计
	go f.reportStats(ctx)

	for {
		if ctx.Err() != nil {
			return nil
		}

		m, err := f.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("拉取消息失败: %w", err)
		}

		// 限速: 在写入前等待令牌
		if err := f.waitRateLimit(ctx, len(m.Key)+len(m.Value)); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("限速等待失败: %w", err)
		}

		out := kafka.Message{
			Key:     m.Key,
			Value:   m.Value,
			Headers: m.Headers,
		}
		if err := f.writer.WriteMessages(ctx, out); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("写入目标 topic 失败: %w", err)
		}

		// 写入成功后提交源端位点, 保证至少一次语义
		if err := f.reader.CommitMessages(ctx, m); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("提交位点失败: %w", err)
		}

		f.totalMsgs++
		f.totalBytes += uint64(len(m.Key) + len(m.Value))
	}
}

// waitRateLimit 根据配置在必要时阻塞等待令牌
func (f *Forwarder) waitRateLimit(ctx context.Context, msgBytes int) error {
	if f.msgLim != nil {
		if err := f.msgLim.Wait(ctx); err != nil {
			return err
		}
	}
	if f.byteLim != nil {
		n := msgBytes
		if n <= 0 {
			n = 1
		}
		// 单条消息可能超过突发容量, 分段消费令牌避免 WaitN 报错
		for n > 0 {
			take := n
			if take > f.byteLim.Burst() {
				take = f.byteLim.Burst()
			}
			if err := f.byteLim.WaitN(ctx, take); err != nil {
				return err
			}
			n -= take
		}
	}
	return nil
}

func (f *Forwarder) reportStats(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("已转发消息: %d 条, %d 字节", f.totalMsgs, f.totalBytes)
		}
	}
}

func (f *Forwarder) close() {
	if err := f.reader.Close(); err != nil {
		log.Printf("关闭 reader 出错: %v", err)
	}
	if err := f.writer.Close(); err != nil {
		log.Printf("关闭 writer 出错: %v", err)
	}
	log.Printf("转发结束, 累计: %d 条, %d 字节", f.totalMsgs, f.totalBytes)
}
