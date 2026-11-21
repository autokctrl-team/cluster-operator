/*
RabbitMQ Cluster Operator

Copyright 2020 VMware, Inc. All Rights Reserved.

This product is licensed to you under the Mozilla Public license, Version 2.0 (the "License").  You may not use this product except in compliance with the Mozilla Public License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package rabbitmqconfig

import (
	"fmt"
	"strings"

	"gopkg.in/ini.v1"
)

// ManagementPathPrefix reads the management path prefix from additionalConfig.
func ManagementPathPrefix(additionalConfig string) (string, error) {
	iniFile, err := ini.Load([]byte(additionalConfig))
	if err != nil {
		return "", fmt.Errorf("failed to load spec.rabbitmq.additionalConfig: %w", err)
	}

	section := iniFile.Section("")
	if !section.HasKey("management.path_prefix") {
		return "", nil
	}

	prefix := strings.Trim(section.Key("management.path_prefix").String(), "/")
	if prefix == "" {
		return "", nil
	}
	return "/" + prefix, nil
}
