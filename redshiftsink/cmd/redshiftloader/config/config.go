package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"

	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
)

type Config struct {
	Loader            redshiftloader.LoaderConfig `yaml:"loader"`
	Kafka             consumer.KafkaConfig        `yaml:"kafka"`
	Sarama            consumer.SaramaConfig       `yaml:"sarama"`
	S3Sink            s3sink.Config               `yaml:"s3sink"`
	SchemaRegistryURL string                      `yaml:"schemaRegistryURL"`
	Redshift          redshift.RedshiftConfig     `yaml:"redshift"`
	Reload            bool                        `yaml:"reload"`
}

func LoadConfig(cmd *cobra.Command) (Config, error) {
	c := Config{}

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return c, err
	}

	configName := "config.yaml"
	relativePath := "./cmd/redshiftloader/config"

	viper.SetEnvPrefix("loader")
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
