package s3select

import "strconv"

type PathPartKind int

const (
	PathProp PathPartKind = iota + 1
	PathArray
	PathWildcard
)

type FromKind int

const (
	FromArchive FromKind = iota + 1
	FromObject
	FromS3Object
)

type PathPart struct {
	Kind   PathPartKind
	Index  int
	Name   string
	NoCase bool
}

type Field struct {
	Path []PathPart
}

type SelectQuery struct {
	currentPath []PathPart

	Fields   []Field
	FromKind FromKind
	FromPath []PathPart
	Alias    string
}

func (s *SelectQuery) FromArchive() {
	s.FromKind = FromArchive
}

func (s *SelectQuery) FromObject() {
	s.FromKind = FromObject
	s.eatPath()
}

func (s *SelectQuery) FromS3ObjectPath() {
	s.FromKind = FromS3Object
	s.FromPath = s.eatPath()
}

func (s *SelectQuery) eatPath() (pp []PathPart) {
	pp, s.currentPath = s.currentPath, nil
	return pp
}

func (s *SelectQuery) RegisterAlias(text string) {
	s.Alias = text
}

func (s *SelectQuery) AppendField() {
	s.Fields = append(s.Fields, Field{Path: s.eatPath()})
}

func (s *SelectQuery) PathObjectPropNoCase(text string) {
	s.currentPath = append(s.currentPath, PathPart{Kind: PathProp, Name: text, NoCase: true})
}

func (s *SelectQuery) PathObjectProp(text string) {
	s.currentPath = append(s.currentPath, PathPart{Kind: PathProp, Name: text})
}

func (s *SelectQuery) PathWildcardIndex() {
	s.currentPath = append(s.currentPath, PathPart{Kind: PathWildcard})
}

func (s *SelectQuery) PathArrayIndex(text string) {
	idx, _ := strconv.ParseInt(text, 10, 0)
	s.currentPath = append(s.currentPath, PathPart{Kind: PathArray, Index: int(idx)})
}
