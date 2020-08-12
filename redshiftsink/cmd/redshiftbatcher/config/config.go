package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/s3sink"
)

type Config struct {
	Batcher consumer.BatcherConfig `yaml: batcher`
	Kafka   consumer.KafkaConfig   `yaml: kafka`
	Sarama  consumer.SaramaConfig  `yaml: sarama`
	S3Sink  s3sink.Config          `yaml: s3sink`
}

func LoadConfig(cmd *cobra.Command) (Config, error) {
	c := Config{}

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return c, err
	}

	configName := "config.yaml"
	relativePath := "./cmd/redshiftbatcher/config"

	viper.AutomaticEnv()
	viper.SetDefault("kafak.version", "2.5.0")

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(relativePath + "/" + configFile)
		configName = configFile
	} else {
		viper.AddConfigPath(relativePath)
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	if err := viper.ReadInConfig(); err != nil {
		return c, err
	}
	if err := viper.Unmarshal(&c); err != nil {
		return c, err
	}

	ValidateConfig(c)

	klog.Infof("Using config file: %s\n", configName)
	klog.V(5).Infof("ConfigFilePath: %s\n", viper.ConfigFileUsed())
	klog.V(5).Infof("Config: %+v\n", c)
	return c, nil
}

func ValidateConfig(config Config) error {
	return nil
}
