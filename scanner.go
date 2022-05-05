package qry

type Scanner interface {
	Scan(dest ...any) error
}
