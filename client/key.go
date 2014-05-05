// Public Domain (-) 2013 The Ampstore Authors.
// See the Ampstore UNLICENSE file for details.
//
// Parts of this file are adapted from the Apache 2.0
// licensed App Engine Go SDK file by Google:
//
//     appengine/datastore/key.go

package ampstore

// Key represents the datastore key for a stored item. They
// are generally created using the Key() and AutoID()
// methods of DB instances, but can be created explicitly
// too.
type Key struct {
	IntID     int64
	Kind      string
	Namespace string
	Parent    *Key
	StringID  string
}

// Equals returns whether two keys are equal.
func (k *Key) Equals(o *Key) bool {
	for k != nil && o != nil {
		if k.Kind != o.Kind || k.StringID != o.StringID || k.IntID != o.IntID || k.Namespace != o.Namespace {
			return false
		}
		k, o = k.Parent, o.Parent
	}
	return k == o
}

// Root returns the root key for the key's ancestry. If a
// key's Parent is nil, then the key is considered its own
// root key.
func (k *Key) Root() *Key {
	for k.Parent != nil {
		k = k.Parent
	}
	return k
}

func (k *Key) marshal(b *bytes.Buffer) {
	if k.Parent != nil {
		k.Parent.marshal(b)
	}
	b.WriteByte('/')
	b.WriteString(k.Kind)
	b.WriteByte('·')
	if k.StringID != "" {
		b.WriteString(k.StringID)
	} else {
		b.WriteString(strconv.FormatInt(k.IntID, 10))
	}
}

// String returns a string representation of the key.
func (k *Key) String() string {
	if k == nil {
		return ""
	}
	b := bytes.NewBuffer(make([]byte, 0, 512))
	k.marshal(b)
	return b.String()
}

// WithNamespace provides a fluent API for setting the key's
// Namespace field.
func (k *Key) WithNamespace(ns string) *Key {
	k.Namespace = ns
	return k
}

// WithParent provides a fluent API for setting the key's
// Parent field.
func (k *Key) WithParent(p *Key) *Key {
	k.Parent = p
	return k
}
