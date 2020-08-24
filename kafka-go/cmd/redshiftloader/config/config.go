package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/consumer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshiftloader"
)

type Config struct {
	Loader            redshiftloader.LoaderConfig `yaml: loader`
	Kafka             consumer.KafkaConfig        `yaml: kafka`
	Sarama            consumer.SaramaConfig       `yaml: sarama`
	SchemaRegistryURL string                      `yaml: schemaRegistryURL`
}

func LoadConfig(cmd *cobra.Command) (Config, error) {
	c := Config{}

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return c, err
	}

	configName := "config.yaml"
	relativePath := "./cmd/redshiftloader/config"

	viper.AutomaticEnv()
	viper.SetDefault("kafka.version", "2.5.0")

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