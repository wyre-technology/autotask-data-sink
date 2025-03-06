package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	Autotask AutotaskConfig `mapstructure:"autotask"`
	Database DatabaseConfig `mapstructure:"database"`
	Sync     SyncConfig     `mapstructure:"sync"`
	Log      LogConfig      `mapstructure:"log"`
}

// AutotaskConfig holds Autotask API credentials
type AutotaskConfig struct {
	Username        string `mapstructure:"username"`
	Secret          string `mapstructure:"secret"`
	IntegrationCode string `mapstructure:"integration_code"`
}

// DatabaseConfig holds database connection details
type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Name     string `mapstructure:"name"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

// ConnectionString returns a PostgreSQL connection string
func (d DatabaseConfig) ConnectionString() string {
	// Check for environment variables first
	host := os.Getenv("PGHOST")
	if host == "" {
		host = d.Host
	}

	portStr := os.Getenv("PGPORT")
	port := d.Port
	if portStr != "" {
		// Parse the port from the environment variable
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	dbname := os.Getenv("PGDATABASE")
	if dbname == "" {
		dbname = d.Name
	}

	user := os.Getenv("PGUSER")
	if user == "" {
		user = d.User
	}

	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = d.Password
	}

	sslmode := os.Getenv("PGSSLMODE")
	if sslmode == "" {
		sslmode = d.SSLMode
	}

	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		host, port, dbname, user, password, sslmode)
}

// SyncConfig holds sync frequencies for different entity types
type SyncConfig struct {
	Tickets     int `mapstructure:"tickets"`      // in minutes
	TimeEntries int `mapstructure:"time_entries"` // in minutes
	Companies   int `mapstructure:"companies"`    // in minutes
	Contacts    int `mapstructure:"contacts"`     // in minutes
	Resources   int `mapstructure:"resources"`    // in minutes
	Contracts   int `mapstructure:"contracts"`    // in minutes
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load loads the configuration from file and environment variables
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// Set default values
	setDefaults()

	// Read from environment variables
	viper.SetEnvPrefix("AUTOTASK_SINK")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Read the config file
	if err := viper.ReadInConfig(); err != nil {
		// It's okay if config file doesn't exist
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// Database defaults
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.name", "autotask_data")
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.ssl_mode", "disable")

	// Sync frequency defaults (in minutes)
	viper.SetDefault("sync.tickets", 15)
	viper.SetDefault("sync.time_entries", 15)
	viper.SetDefault("sync.companies", 1440) // Daily
	viper.SetDefault("sync.contacts", 1440)  // Daily
	viper.SetDefault("sync.resources", 1440) // Daily
	viper.SetDefault("sync.contracts", 1440) // Daily

	// Log defaults
	viper.SetDefault("log.level", "info")
	viper.SetDefault("log.format", "json")
}
