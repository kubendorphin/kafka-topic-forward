package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 程序总配置
type Config struct {
	Source    EndpointConfig  `yaml:"source"`     // 源 kafka
	Dest      EndpointConfig  `yaml:"dest"`       // 目标 kafka
	Forward   ForwardConfig   `yaml:"forward"`    // 转发相关参数
	RateLimit RateLimitConfig `yaml:"rate_limit"` // 限速参数
}

// EndpointConfig 一个 kafka 端点(源或目标)
type EndpointConfig struct {
	Brokers []string `yaml:"brokers"`  // broker 地址列表, 如 ["127.0.0.1:9092"]
	Topic   string   `yaml:"topic"`    // topic 名称
	GroupID string   `yaml:"group_id"` // 消费组(仅源端使用)
}

// ForwardConfig 转发行为参数
type ForwardConfig struct {
	StartFromEarliest bool          `yaml:"start_from_earliest"` // true=从最早开始, false=从最新开始
	BatchSize         int           `yaml:"batch_size"`          // 生产端批量条数
	BatchTimeout      time.Duration `yaml:"batch_timeout"`       // 生产端批量最大等待时间
	CommitInterval    time.Duration `yaml:"commit_interval"`     // 消费位点提交间隔
	MinBytes          int           `yaml:"min_bytes"`           // 单次拉取最小字节
	MaxBytes          int           `yaml:"max_bytes"`           // 单次拉取最大字节
}

// RateLimitConfig 限速参数
type RateLimitConfig struct {
	Enabled           bool `yaml:"enabled"`             // 是否开启限速
	MessagesPerSecond int  `yaml:"messages_per_second"` // 每秒转发消息条数上限, <=0 表示不按条数限速
	BytesPerSecond    int  `yaml:"bytes_per_second"`    // 每秒转发字节上限, <=0 表示不按字节限速
	Burst             int  `yaml:"burst"`               // 令牌桶突发容量, <=0 时自动取速率值
}

// LoadConfig 从文件加载配置并填充默认值
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	if err := cfg.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaultsAndValidate() error {
	if len(c.Source.Brokers) == 0 {
		return fmt.Errorf("source.brokers 不能为空")
	}
	if c.Source.Topic == "" {
		return fmt.Errorf("source.topic 不能为空")
	}
	if len(c.Dest.Brokers) == 0 {
		return fmt.Errorf("dest.brokers 不能为空")
	}
	if c.Dest.Topic == "" {
		return fmt.Errorf("dest.topic 不能为空")
	}
	if c.Source.GroupID == "" {
		c.Source.GroupID = "kafka-topic-forward"
	}

	if c.Forward.BatchSize <= 0 {
		c.Forward.BatchSize = 100
	}
	if c.Forward.BatchTimeout <= 0 {
		c.Forward.BatchTimeout = time.Second
	}
	if c.Forward.CommitInterval <= 0 {
		c.Forward.CommitInterval = time.Second
	}
	if c.Forward.MinBytes <= 0 {
		c.Forward.MinBytes = 1
	}
	if c.Forward.MaxBytes <= 0 {
		c.Forward.MaxBytes = 10 << 20 // 10MB
	}

	if c.RateLimit.Enabled && c.RateLimit.MessagesPerSecond <= 0 && c.RateLimit.BytesPerSecond <= 0 {
		return fmt.Errorf("已开启限速但 messages_per_second 与 bytes_per_second 均未设置")
	}
	return nil
}
