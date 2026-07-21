package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"golang.org/x/time/rate"
)

// Forwarder 负责从源 topic 消费并转发到目标 topic
type Forwarder struct {
	cfg     *Config
	reader  *kafka.Reader
	writer  *kafka.Writer
	msgLim  *rate.Limiter // 按条数限速
	byteLim *rate.Limiter // 按字节限速

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

	sourceDialer := buildDialer(&cfg.Source)
	destDialer := buildDialer(&cfg.Dest)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Source.Brokers,
		Topic:          cfg.Source.Topic,
		GroupID:        cfg.Source.GroupID,
		StartOffset:    startOffset,
		MinBytes:       cfg.Forward.MinBytes,
		MaxBytes:       cfg.Forward.MaxBytes,
		CommitInterval: cfg.Forward.CommitInterval,
		Dialer:         sourceDialer,
		MaxWait:        2 * time.Second,
	})

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Dest.Brokers...),
		Topic:    cfg.Dest.Topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			TLS:  destDialer.TLS,
			SASL: destDialer.SASLMechanism,
		},
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

// buildDialer 根据端点配置创建带认证能力的 Dialer
func buildDialer(ep *EndpointConfig) *kafka.Dialer {
	d := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// TLS
	if ep.TLS.Enable {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: ep.TLS.InsecureSkipVerify,
		}
		if ep.TLS.CACertFile != "" || ep.TLS.CertFile != "" || ep.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(ep.TLS.CertFile, ep.TLS.KeyFile)
			if err != nil {
				log.Printf("警告: 加载客户端证书失败: %v", err)
			} else {
				tlsCfg.Certificates = []tls.Certificate{cert}
			}
		}
		if ep.TLS.CACertFile != "" {
			caCert, err := os.ReadFile(ep.TLS.CACertFile)
			if err != nil {
				log.Printf("警告: 读取 CA 证书失败: %v", err)
			} else {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(caCert) {
					tlsCfg.RootCAs = pool
				} else {
					log.Printf("警告: CA 证书解析失败")
				}
			}
		}
		d.TLS = tlsCfg
	}

	// SASL
	if ep.Auth.Mechanism != "" {
		var mech sasl.Mechanism
		switch ep.Auth.Mechanism {
		case "plain":
			mech = plain.Mechanism{
				Username: ep.Auth.Username,
				Password: ep.Auth.Password,
			}
		case "scram-sha-256":
			m, err := scram.Mechanism(scram.SHA256, ep.Auth.Username, ep.Auth.Password)
			if err != nil {
				log.Printf("警告: 创建 SCRAM-SHA-256 机制失败: %v", err)
			} else {
				mech = m
			}
		case "scram-sha-512":
			m, err := scram.Mechanism(scram.SHA512, ep.Auth.Username, ep.Auth.Password)
			if err != nil {
				log.Printf("警告: 创建 SCRAM-SHA-512 机制失败: %v", err)
			} else {
				mech = m
			}
		default:
			log.Printf("警告: 不支持的 SASL mechanism: %s", ep.Auth.Mechanism)
		}
		if mech != nil {
			d.SASLMechanism = mech
		}
	}

	return d
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

	// 批量缓冲区
	batchSize := f.cfg.Forward.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	batchTimeout := f.cfg.Forward.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 1 * time.Second
	}

	batch := make([]kafka.Message, 0, batchSize)
	commitBatch := make([]kafka.Message, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// 批量写入
		if err := f.writer.WriteMessages(ctx, batch...); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("批量写入失败 (%d 条), 等待后重试: %v", len(batch), err)
			time.Sleep(5 * time.Second)
			return err
		}

		// 批量提交位点
		if len(commitBatch) > 0 {
			if err := f.reader.CommitMessages(ctx, commitBatch...); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("批量提交位点失败: %v", err)
				}
			}
		}

		// 更新统计
		f.totalMsgs += uint64(len(batch))
		for i := range batch {
			f.totalBytes += uint64(len(batch[i].Key) + len(batch[i].Value))
		}

		// 清空批次
		batch = batch[:0]
		commitBatch = commitBatch[:0]
		ticker.Reset(batchTimeout)

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// 退出前尝试刷新剩余消息
			flushBatch()
			return nil

		case <-ticker.C:
			// 超时刷新
			if err := flushBatch(); err != nil && errors.Is(err, context.Canceled) {
				return nil
			}

		default:
			// 非阻塞拉取消息
			m, err := f.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					flushBatch()
					return nil
				}
				log.Printf("拉取消息失败, 等待后重试: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			// 限速: 在加入批次前等待令牌
			msgSize := len(m.Key) + len(m.Value)
			if err := f.waitRateLimit(ctx, msgSize); err != nil {
				if errors.Is(err, context.Canceled) {
					flushBatch()
					return nil
				}
				log.Printf("限速等待失败: %v", err)
				continue
			}

			// 加入批次
			batch = append(batch, kafka.Message{
				Key:     m.Key,
				Value:   m.Value,
				Headers: m.Headers,
			})
			commitBatch = append(commitBatch, m)

			// 批次满了则立即刷新
			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil && errors.Is(err, context.Canceled) {
					return nil
				}
			}
		}
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
