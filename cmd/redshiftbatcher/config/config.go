package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/pkg/kafka"
	"github.com/practo/tipoca-stream/pkg/redshiftbatcher"
	"github.com/practo/tipoca-stream/pkg/s3sink"
)

type Config struct {
	Batcher           redshiftbatcher.BatcherConfig `yaml:"batcher"`
	ConsumerGroups    []kafka.ConsumerGroupConfig   `yaml:"consumerGroups"`
	S3Sink            s3sink.Config                 `yaml:"s3sink"`
	SchemaRegistryURL string                        `yaml:"schemaRegistryURL"`
	GitAccessToken    string                        `yaml:"gitAccessToken"`
	SinkGroup         string                        `yaml:"sinkGroup,omitempty"`
}

func LoadConfig(cmd *cobra.Command) (Config, error) {
	c := Config{}

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return c, err
	}

	configName := "config.yaml"
	relativePath := "./cmd/redshiftbatcher/config"

	viper.SetEnvPrefix("batcher")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	viper.SetDefault("kafka.version", "2.5.0")

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
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
	klog.V(4).Infof("ConfigFilePath: %s\n", viper.ConfigFileUsed())
	klog.V(5).Infof("Config: %+v\n", c)
	return c, nil
}

func ValidateConfig(config Config) error {
	return nil
}
