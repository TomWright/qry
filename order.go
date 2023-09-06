package qry

import "fmt"

type Direction string

func (f Direction) String() string {
	return string(f)
}

const Ascending Direction = "ASC"
const Descending Direction = "DESC"

type OrderBy struct {
	Field     Field
	Direction Direction
}

func (ob OrderBy) String() string {
	return fmt.Sprintf("%s %s", ob.Field.String(), ob.Direction.String())
}
