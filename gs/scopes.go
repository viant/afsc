package gs

//Scopes represents storage scopes
type Scopes []string

//NewScopes create scopes
func NewScopes(scopes ...string) Scopes {
	return scopes
}
