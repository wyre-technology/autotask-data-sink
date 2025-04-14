package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
	Pool     struct {
		MaxOpenConns    int           `mapstructure:"max_open_conns"`
		MaxIdleConns    int           `mapstructure:"max_idle_conns"`
		ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
		ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
	} `mapstructure:"pool"`
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
	BatchSize   int `mapstructure:"batch_size"`   // number of records per batch
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate Autotask configuration
	if c.Autotask.Username == "" {
		return fmt.Errorf("autotask username is required")
	}
	if c.Autotask.Secret == "" {
		return fmt.Errorf("autotask secret is required")
	}
	if c.Autotask.IntegrationCode == "" {
		return fmt.Errorf("autotask integration code is required")
	}

	// Validate Database configuration
	if c.Database.Host == "" {
		return fmt.Errorf("database host is required")
	}
	if c.Database.Port <= 0 {
		return fmt.Errorf("invalid database port")
	}
	if c.Database.Name == "" {
		return fmt.Errorf("database name is required")
	}
	if c.Database.User == "" {
		return fmt.Errorf("database user is required")
	}
	if c.Database.SSLMode == "" {
		return fmt.Errorf("database ssl_mode is required")
	}

	// Validate Sync configuration
	if c.Sync.Tickets <= 0 {
		return fmt.Errorf("invalid tickets sync frequency")
	}
	if c.Sync.TimeEntries <= 0 {
		return fmt.Errorf("invalid time entries sync frequency")
	}
	if c.Sync.Companies <= 0 {
		return fmt.Errorf("invalid companies sync frequency")
	}
	if c.Sync.Contacts <= 0 {
		return fmt.Errorf("invalid contacts sync frequency")
	}
	if c.Sync.Resources <= 0 {
		return fmt.Errorf("invalid resources sync frequency")
	}
	if c.Sync.Contracts <= 0 {
		return fmt.Errorf("invalid contracts sync frequency")
	}

	// Validate Log configuration
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLevels[c.Log.Level] {
		return fmt.Errorf("invalid log level: %s", c.Log.Level)
	}

	validFormats := map[string]bool{
		"json":    true,
		"console": true,
	}
	if !validFormats[c.Log.Format] {
		return fmt.Errorf("invalid log format: %s", c.Log.Format)
	}

	return nil
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
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Map environment variables to config fields
	viper.BindEnv("autotask.username", "AUTOTASK_USERNAME")
	viper.BindEnv("autotask.secret", "AUTOTASK_SECRET")
	viper.BindEnv("autotask.integration_code", "AUTOTASK_INTEGRATION_CODE")
	viper.BindEnv("sync.tickets", "SYNC_TICKETS")
	viper.BindEnv("sync.time_entries", "SYNC_TIME_ENTRIES")
	viper.BindEnv("sync.companies", "SYNC_COMPANIES")
	viper.BindEnv("sync.contacts", "SYNC_CONTACTS")
	viper.BindEnv("sync.resources", "SYNC_RESOURCES")
	viper.BindEnv("sync.contracts", "SYNC_CONTRACTS")
	viper.BindEnv("sync.batch_size", "SYNC_BATCH_SIZE")
	viper.BindEnv("log.level", "LOG_LEVEL")
	viper.BindEnv("log.format", "LOG_FORMAT")

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

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
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
	viper.SetDefault("database.pool.max_open_conns", 25)
	viper.SetDefault("database.pool.max_idle_conns", 5)
	viper.SetDefault("database.pool.conn_max_lifetime", time.Hour)
	viper.SetDefault("database.pool.conn_max_idle_time", time.Minute*5)

	// Sync frequency defaults (in minutes)
	viper.SetDefault("sync.tickets", 15)
	viper.SetDefault("sync.time_entries", 15)
	viper.SetDefault("sync.companies", 1440) // Daily
	viper.SetDefault("sync.contacts", 1440)  // Daily
	viper.SetDefault("sync.resources", 1440) // Daily
	viper.SetDefault("sync.contracts", 1440) // Daily
	viper.SetDefault("sync.batch_size", 100) // Default batch size

	// Log defaults
	viper.SetDefault("log.level", "info")
	viper.SetDefault("log.format", "json")
}
