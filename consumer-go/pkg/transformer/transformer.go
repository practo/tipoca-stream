package transformer

type Transformer interface {
    Transfrom(message string) string
}

func New() Transformer {
    return &redshiftTransformer{}
}

type redshiftTransformer struct {}

func (* redshiftTransformer) Transform(message string) string {
    return ""
}
