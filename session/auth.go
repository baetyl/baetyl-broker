package session

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/baetyl/baetyl-go/mqtt"
	"gopkg.in/validator.v2"
)

// all permit actions
const (
	Publish   = "pub"
	Subscribe = "sub"
)

// Permission Permission
type Permission struct {
	Action  string   `yaml:"action" json:"action" validate:"regexp=^(p|s)ub$"`
	Permits []string `yaml:"permit,flow" json:"permit,flow"`
}

// Principal Principal
type Principal struct {
	Username    string       `yaml:"username" json:"username"`
	Password    string       `yaml:"password" json:"password"`
	Permissions []Permission `yaml:"permissions" json:"permissions"`
}

// Authenticator authenticator
type Authenticator struct {
	// for client account
	accounts map[string]account
	// for client certificates
	certificates map[string]certificate
}

// NewAuthenticator creates a new Authenticator
func NewAuthenticator(principals []Principal) *Authenticator {
	if len(principals) == 0 {
		return nil
	}
	_accounts := make(map[string]account)
	_certificates := make(map[string]certificate)
	for _, principal := range principals {
		authorizer := NewAuthorizer()
		for _, p := range duplicatePubSubPermitRemove(principal.Permissions) {
			for _, topic := range p.Permits {
				authorizer.Add(topic, p.Action)
			}
		}
		if principal.Password == "" {
			_certificates[principal.Username] = certificate{
				Authorizer: authorizer,
			}
		} else {
			_accounts[principal.Username] = account{
				Password:   principal.Password,
				Authorizer: authorizer,
			}
		}
	}
	return &Authenticator{certificates: _certificates, accounts: _accounts}
}

func duplicatePubSubPermitRemove(permission []Permission) []Permission {
	PubPermitList := make(map[string]struct{})
	SubPermitList := make(map[string]struct{})
	for _, _permission := range permission {
		switch _permission.Action {
		case Publish:
			for _, v := range _permission.Permits {
				PubPermitList[v] = struct{}{}
			}
		case Subscribe:
			for _, v := range _permission.Permits {
				SubPermitList[v] = struct{}{}
			}
		}
	}
	return []Permission{
		{Action: Publish, Permits: getKeys(PubPermitList)},
		{Action: Subscribe, Permits: getKeys(SubPermitList)},
	}
}

// AuthenticateAccount authenticates client account, then return authorizer if pass
func (a *Authenticator) AuthenticateAccount(username, password string) *Authorizer {
	if len(password) == 0 {
		return nil
	}
	c, ok := a.accounts[username]
	if !ok {
		return nil
	}
	if strings.Compare(password, c.Password) != 0 {
		return nil
	}
	return c.Authorizer
}

// AuthenticateCertificate authenticates client certificate, then return authorizer if pass
func (a *Authenticator) AuthenticateCertificate(commonName string) *Authorizer {
	c, ok := a.certificates[commonName]
	if ok {
		return c.Authorizer
	}
	return nil
}

type account struct {
	Password   string
	Authorizer *Authorizer
}

type certificate struct {
	Authorizer *Authorizer
}

// Authorizer checks topic permission
type Authorizer struct {
	*mqtt.Trie
}

// NewAuthorizer create a new authorizer
func NewAuthorizer() *Authorizer {
	return &Authorizer{Trie: mqtt.NewTrie()}
}

// Authorize auth action
func (p *Authorizer) Authorize(action, topic string) bool {
	_actions := p.Match(topic)
	for _, _action := range _actions {
		if action == _action.(string) {
			return true
		}
	}
	return false
}

// getKeys gets all keys of map
func getKeys(m map[string]struct{}) []string {
	keys := reflect.ValueOf(m).MapKeys()
	result := make([]string, 0)
	for _, key := range keys {
		result = append(result, key.Interface().(string))
	}
	return result
}

func init() {
	validator.SetValidationFunc("principals", principalsValidate)
}

// principalsValidate validate principals config is valid or not
func principalsValidate(v interface{}, param string) error {
	if v == nil {
		return nil
	}
	principals := v.([]Principal)
	if len(principals) == 0 {
		return nil
	}
	err := userValidate(principals)
	if err != nil {
		return err
	}
	for _, principal := range principals {
		for _, permission := range principal.Permissions {
			for _, permit := range permission.Permits {
				if !mqtt.CheckTopic(permit, true) {
					return fmt.Errorf("%s topic(%s) invalid", permission.Action, permit)
				}
			}
		}
	}
	return nil
}

// userValidate validate username duplicate or not
func userValidate(principals []Principal) error {
	userMap := make(map[string]struct{})
	for _, principal := range principals {
		if _, ok := userMap[principal.Username]; ok {
			return fmt.Errorf("username (%s) duplicate", principal.Username)
		}
		userMap[principal.Username] = struct{}{}
	}

	return nil
}
