package config

import (
	"os"

	"github.com/go-playground/errors"
	"gopkg.in/yaml.v3"
)

func ReadConnectorYAML(path string) (*Connector, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "read yaml config")
	}
	var c Connector
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, errors.Wrap(err, "yaml parse")
	}
	return &c, nil
}
