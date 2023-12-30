package common

type Tuple[V1 any, V2 any] struct {
	First  V1
	Second V2
}

type Tuple3[V1 any, V2 any, V3 any] struct {
	First  V1
	Second V2
	Third  V3
}

type Tuple4[V1 any, V2 any, V3 any, V4 any] struct {
	First  V1
	Second V2
	Third  V3
	Fourth V4
}
