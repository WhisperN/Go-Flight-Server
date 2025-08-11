package config

import (
	"github.com/goccy/go-yaml"
	"github.com/sirupsen/logrus"
	"os"
)

type ServerConfig struct {
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
}

type DuckDBConfig struct {
	Driver      string `yaml:"driver"`
	Entrypoint  string `yaml:"entrypoint"`
	Path        string `yaml:"path"`
	TableSource string `yaml:"source"`
	TableName   string `yaml:"name"`
}

type Config struct {
	Server ServerConfig `yaml:"server"`
	DuckDB DuckDBConfig `yaml:"duckdb"`
}

func LoadConfig(dev bool) *Config {
	y, err := os.Open("config.yaml")
	if err != nil {
		panic("Error loading config.yaml")
	}
	defer func(y *os.File) {
		err := y.Close()
		if err != nil {
			panic(err)
		}
	}(y)
	logrus.Infof("Loaded config.yaml")
	var cfg Config
	err = yaml.NewDecoder(y).Decode(&cfg)
	if err != nil {
		panic(err)
	}
	return &cfg
}
