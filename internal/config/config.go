package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

type Config struct {
	PubSub PubSubConfig `yaml:"pubsub"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	App    AppConfig    `yaml:"app"`
}

type PubSubConfig struct {
	ProjectID       string `yaml:"project_id"`
	SubscriptionID  string `yaml:"subscription_id"`
	CredentialsFile string `yaml:"credentials_file"`
}

type KafkaConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers"`
	SecurityProtocol string `yaml:"security_protocol"`
	SASLMechanisms   string `yaml:"sasl_mechanisms"` // Changed from "sasl_mechanisms"
	SASLUsername     string `yaml:"sasl_username"`   // Changed from "sasl_username"
	SASLPassword     string `yaml:"sasl_password"`   // Changed from "sasl_password"
	Topic            string `yaml:"topic"`
}

type AppConfig struct {
	Name     string `yaml:"name"`
	LogLevel string `yaml:"log_level"`
}

func LoadConfig(configPath string) (*Config, error) {
	config := &Config{}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("error closing config file: %v", err)
		}
	}()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		return nil, err
	}

	// Override with environment variables if present
	if envProjectID := os.Getenv("PUBSUB_PROJECT_ID"); envProjectID != "" {
		config.PubSub.ProjectID = envProjectID
	}
	if envCredentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); envCredentials != "" {
		config.PubSub.CredentialsFile = envCredentials
	}

	// Validate required fields
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

func validateConfig(config *Config) error {
	// Validate Kafka config
	if config.Kafka.BootstrapServers == "" {
		return fmt.Errorf("kafka bootstrap_servers is required")
	}
	if config.Kafka.SecurityProtocol == "" {
		return fmt.Errorf("kafka security_protocol is required")
	}
	if config.Kafka.SASLMechanisms == "" {
		return fmt.Errorf("kafka sasl_mechanisms is required")
	}
	if config.Kafka.SASLUsername == "" {
		return fmt.Errorf("kafka sasl_username is required")
	}
	if config.Kafka.SASLPassword == "" {
		return fmt.Errorf("kafka sasl_password is required")
	}
	if config.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is required")
	}

	// Validate PubSub config
	if config.PubSub.ProjectID == "" {
		return fmt.Errorf("pubsub project_id is required")
	}
	if config.PubSub.SubscriptionID == "" {
		return fmt.Errorf("pubsub subscription_id is required")
	}

	return nil
}
