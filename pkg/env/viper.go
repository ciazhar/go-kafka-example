package env

import (
	"fmt"
	"github.com/spf13/viper"
)

func Init(name string) {
	viper.SetConfigName(name)
	viper.SetConfigType("env")
	viper.AddConfigPath("config")
	err := viper.ReadInConfig() // Find and read the env deployments
	if err != nil {             // Handle errors reading the env deployments
		panic(fmt.Errorf("Fatal error env deployments: %w \n", err))
	}
}
