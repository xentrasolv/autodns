// Copyright 2025 Jelly Terra <jellyterra@proton.me>
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0
// that can be found in the LICENSE file and https://mozilla.org/MPL/2.0/.

package core

import (
	"fmt"
	"sync"

	"golang.org/x/net/idna"
)

const (
	OP_UPDATE = "update"
	OP_DELETE = "delete"
)

type Operation struct {
	Record
	Op        string `json:"op"`
	Domain    string `json:"domain"`
	Subdomain string `json:"subdomain"`

	Registry string
}

func ValidateOperation(roleDef *RoleDef, op *Operation) error {
	result, err := Validate(roleDef, op.Domain, op.Subdomain)
	if err != nil {
		return err
	}

	op.Registry = result.Registry

	op.Domain, err = idna.ToASCII(op.Domain)
	if err != nil {
		return err
	}

	if op.Subdomain == "" {
		op.CanonicalName = op.Domain
	} else {
		op.Subdomain, err = idna.ToASCII(op.Subdomain)
		if err != nil {
			return err
		}

		op.CanonicalName = op.Subdomain + "." + op.Domain
	}

	return nil
}

func ExecuteAll(c *Context, roleDef *RoleDef, operations []*Operation, callback func(err error, op *Operation)) error {

	// Authorize and check.

	for _, op := range operations {
		err := ValidateOperation(roleDef, op)
		if err != nil {
			return err
		}
	}

	// Build registries.

	registries := make(map[string]Registry)

	for _, op := range operations {
		registryDef, err := Query(c, &RegistryDef{}, "registry", op.Registry)
		if err != nil {
			return err
		}

		builder := RegistryBuilders[registryDef.Builder]
		if builder == nil {
			return fmt.Errorf("registry [%s] builder [%s] is not builtin", op.Registry, registryDef.Builder)
		}

		registries[op.Registry], err = builder(registryDef.BuilderParams)
		if err != nil {
			return fmt.Errorf("registry [%s] builder [%s] failed: %v", op.Registry, registryDef.Builder, err)
		}
	}

	// Execute operations.

	deleted := map[string][]*Operation{}
	updated := map[string][]*Operation{}

	for _, op := range operations {
		switch op.Op {
		case OP_DELETE:
			deleted[op.Registry] = append(deleted[op.Registry], op)
		case OP_UPDATE:
			updated[op.Registry] = append(updated[op.Registry], op)
		}
	}

	hasBeenDeleted := map[string]bool{}

	var wg sync.WaitGroup
	for registryName, operations := range updated {
		for _, op := range operations {

			// Delete all records with same domain name.
			if hasBeenDeleted[op.CanonicalName] {
				continue
			}
			hasBeenDeleted[op.CanonicalName] = true

			wg.Add(1)
			go func() {
				defer wg.Done()
				err := registries[registryName].DeleteAllRecordsWithDomain(op.CanonicalName)
				if err != nil {
					callback(fmt.Errorf("deleting all records with domain [%s] failed: %v", op.Domain, err), op)
				}
			}()
		}
	}
	wg.Wait()

	for registryName, operations := range updated {
		for _, op := range operations {
			go func() {
				err := registries[registryName].AppendRecord(&op.Record)
				callback(err, op)
			}()
		}
	}

	for registryName, operations := range deleted {
		for _, op := range operations {
			go func() {
				err := registries[registryName].DeleteRecord(&op.Record)
				callback(err, op)
			}()
		}
	}

	return nil
}
