// Package scheduler provides poll scheduling and SNMP polling implementation.
//
// This file implements the SNMPPoller which performs actual SNMP GET operations.
package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/xtxerr/stalker/internal/logging"
)

var snmpLog = logging.Component("snmp")

// =============================================================================
// SNMP Configuration
// =============================================================================

// SNMPConfig holds SNMP poll configuration.
type SNMPConfig struct {
	Host string
	Port uint16
	OID  string

	// v2c
	Community string

	// v3
	SecurityName  string
	SecurityLevel string
	AuthProtocol  string
	AuthPassword  string
	PrivProtocol  string
	PrivPassword  string
	ContextName   string

	// Timing
	TimeoutMs uint32
	Retries   uint32
}

// =============================================================================
// SNMP Poller
// =============================================================================

// SNMPPoller executes SNMP polls.
type SNMPPoller struct {
	defaultTimeoutMs uint32
	defaultRetries   uint32
}

// NewSNMPPoller creates a new SNMP poller.
func NewSNMPPoller(defaultTimeoutMs, defaultRetries uint32) *SNMPPoller {
	return &SNMPPoller{
		defaultTimeoutMs: defaultTimeoutMs,
		defaultRetries:   defaultRetries,
	}
}

// Poll executes an SNMP poll operation.
func (p *SNMPPoller) Poll(ctx context.Context, key PollerKey, cfg *SNMPConfig) PollResult {
	start := time.Now()

	result := PollResult{
		Key:         key,
		TimestampMs: start.UnixMilli(),
	}

	// Validate configuration
	if err := p.validateConfig(cfg); err != nil {
		result.Success = false
		result.Error = err.Error()
		result.PollMs = int(time.Since(start).Milliseconds())
		return result
	}

	// Create SNMP client
	snmp := p.createClient(cfg)

	// Connect
	if err := snmp.Connect(); err != nil {
		result.Success = false
		result.Error = fmt.Sprintf("connect: %v", err)
		result.PollMs = int(time.Since(start).Milliseconds())
		return result
	}
	defer snmp.Conn.Close()

	// Check context before GET
	select {
	case <-ctx.Done():
		result.Success = false
		result.Timeout = true
		result.Error = "context cancelled"
		result.PollMs = int(time.Since(start).Milliseconds())
		return result
	default:
	}

	// Execute GET
	pdu, err := snmp.Get([]string{cfg.OID})
	if err != nil {
		result.Success = false
		result.Error = fmt.Sprintf("get: %v", err)
		if isTimeoutError(err) {
			result.Timeout = true
		}
		result.PollMs = int(time.Since(start).Milliseconds())
		return result
	}

	// Parse result
	if len(pdu.Variables) == 0 {
		result.Success = false
		result.Error = "no variables returned"
		result.PollMs = int(time.Since(start).Milliseconds())
		return result
	}

	// Extract value
	variable := pdu.Variables[0]
	switch variable.Type {
	case gosnmp.Counter32, gosnmp.Counter64, gosnmp.Uinteger32:
		val := gosnmp.ToBigInt(variable.Value).Uint64()
		result.Counter = &val
		result.Success = true

	case gosnmp.Integer:
		val := float64(variable.Value.(int))
		result.Gauge = &val
		result.Success = true

	case gosnmp.OctetString:
		val := string(variable.Value.([]byte))
		result.Text = &val
		result.Success = true

	case gosnmp.TimeTicks:
		val := float64(variable.Value.(uint32))
		result.Gauge = &val
		result.Success = true

	case gosnmp.NoSuchObject, gosnmp.NoSuchInstance:
		result.Success = false
		result.Error = "OID not found"

	default:
		result.Success = false
		result.Error = fmt.Sprintf("unsupported type: %v", variable.Type)
	}

	result.PollMs = int(time.Since(start).Milliseconds())
	return result
}

// =============================================================================
// Configuration Validation
// =============================================================================

// validateConfig validates the SNMP configuration.
//
func (p *SNMPPoller) validateConfig(cfg *SNMPConfig) error {
	if cfg.Host == "" {
		return fmt.Errorf("host is required")
	}
	if cfg.OID == "" {
		return fmt.Errorf("OID is required")
	}

	isV3 := cfg.SecurityName != ""
	if !isV3 && cfg.Community == "" {
		return fmt.Errorf("SNMP v2c requires community string (refusing to use insecure default)")
	}

	return nil
}

// =============================================================================
// SNMP Client Creation
// =============================================================================

func (p *SNMPPoller) createClient(cfg *SNMPConfig) *gosnmp.GoSNMP {
	port := cfg.Port
	if port == 0 {
		port = 161
	}

	timeout := cfg.TimeoutMs
	if timeout == 0 {
		timeout = p.defaultTimeoutMs
	}

	retries := cfg.Retries
	if retries == 0 {
		retries = p.defaultRetries
	}

	snmp := &gosnmp.GoSNMP{
		Target:  cfg.Host,
		Port:    port,
		Timeout: time.Duration(timeout) * time.Millisecond,
		Retries: int(retries),
	}

	// Configure version based on presence of security name
	if cfg.SecurityName != "" {
		snmp.Version = gosnmp.Version3
		snmp.SecurityModel = gosnmp.UserSecurityModel
		snmp.MsgFlags = p.getMsgFlags(cfg.SecurityLevel)
		snmp.SecurityParameters = &gosnmp.UsmSecurityParameters{
			UserName:                 cfg.SecurityName,
			AuthenticationProtocol:   p.getAuthProtocol(cfg.AuthProtocol),
			AuthenticationPassphrase: cfg.AuthPassword,
			PrivacyProtocol:          p.getPrivProtocol(cfg.PrivProtocol),
			PrivacyPassphrase:        cfg.PrivPassword,
		}
		if cfg.ContextName != "" {
			snmp.ContextName = cfg.ContextName
		}
	} else {
		snmp.Version = gosnmp.Version2c
		snmp.Community = cfg.Community
	}

	return snmp
}

// =============================================================================
// SNMPv3 Protocol Helpers
// =============================================================================

func (p *SNMPPoller) getMsgFlags(level string) gosnmp.SnmpV3MsgFlags {
	switch level {
	case "noAuthNoPriv":
		return gosnmp.NoAuthNoPriv
	case "authNoPriv":
		return gosnmp.AuthNoPriv
	case "authPriv":
		return gosnmp.AuthPriv
	default:
		return gosnmp.NoAuthNoPriv
	}
}

func (p *SNMPPoller) getAuthProtocol(protocol string) gosnmp.SnmpV3AuthProtocol {
	switch protocol {
	case "MD5":
		return gosnmp.MD5
	case "SHA":
		return gosnmp.SHA
	case "SHA224":
		return gosnmp.SHA224
	case "SHA256":
		return gosnmp.SHA256
	case "SHA384":
		return gosnmp.SHA384
	case "SHA512":
		return gosnmp.SHA512
	default:
		return gosnmp.NoAuth
	}
}

func (p *SNMPPoller) getPrivProtocol(protocol string) gosnmp.SnmpV3PrivProtocol {
	switch protocol {
	case "DES":
		return gosnmp.DES
	case "AES":
		return gosnmp.AES
	case "AES192":
		return gosnmp.AES192
	case "AES256":
		return gosnmp.AES256
	default:
		return gosnmp.NoPriv
	}
}

// =============================================================================
// Error Helpers
// =============================================================================

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	// gosnmp returns "request timeout" on timeout
	return err.Error() == "request timeout" ||
		err.Error() == "context deadline exceeded"
}
