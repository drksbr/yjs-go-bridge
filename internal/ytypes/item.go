package ytypes

// Content é a menor interface útil para manter a semântica do `Item`.
// Neste estágio ela só precisa informar comprimento e se conta para o tamanho
// lógico do tipo pai, espelhando o contrato mínimo do `AbstractContent` do Yjs.
type Content interface {
	Length() uint32
	IsCountable() bool
}

// ItemFlags preserva os 4 bits baixos usados pelo Yjs em `info`.
type ItemFlags uint8

const (
	ItemFlagKeep ItemFlags = 1 << iota
	ItemFlagCountable
	ItemFlagDeleted
	ItemFlagMarker
)

// Has informa se um bit específico está ativo.
func (f ItemFlags) Has(flag ItemFlags) bool {
	return f&flag != 0
}

// ItemOptions concentra os metadados laterais do item sem poluir o construtor.
type ItemOptions struct {
	Origin      *ID
	RightOrigin *ID
	Parent      ParentRef
	ParentSub   string
	Redone      *ID
	Flags       ItemFlags
}

// Item modela a unidade estrutural principal do update Yjs.
// Ele mantém apenas referências serializáveis, sem acoplamento com o runtime
// completo do documento. Isso é suficiente para o parser e para state vector.
type Item struct {
	baseStruct
	Origin      *ID
	RightOrigin *ID
	Parent      ParentRef
	ParentSub   string
	Redone      *ID
	Info        ItemFlags
	Content     Content
}

// NewItem valida o conteúdo, deriva o comprimento e normaliza o bit de
// countable para refletir o comportamento do Yjs.
func NewItem(id ID, content Content, opts ItemOptions) (*Item, error) {
	return newItem(id, content, opts, true)
}

// NewItemOwnedIDs cria um item assumindo ownership dos ponteiros de ID em opts.
// Use apenas quando os IDs foram recém-materializados para este item e não serão
// mutados pelo caller.
func NewItemOwnedIDs(id ID, content Content, opts ItemOptions) (*Item, error) {
	return newItem(id, content, opts, false)
}

func newItem(id ID, content Content, opts ItemOptions, cloneIDs bool) (*Item, error) {
	if content == nil {
		return nil, ErrNilContent
	}

	base, err := newBaseStruct(id, content.Length())
	if err != nil {
		return nil, err
	}

	info := opts.Flags &^ ItemFlagCountable
	if content.IsCountable() {
		info |= ItemFlagCountable
	}

	origin := opts.Origin
	rightOrigin := opts.RightOrigin
	redone := opts.Redone
	if cloneIDs {
		origin = cloneID(origin)
		rightOrigin = cloneID(rightOrigin)
		redone = cloneID(redone)
	}

	return &Item{
		baseStruct:  base,
		Origin:      origin,
		RightOrigin: rightOrigin,
		Parent:      opts.Parent,
		ParentSub:   opts.ParentSub,
		Redone:      redone,
		Info:        info,
		Content:     content,
	}, nil
}

// Kind identifica a categoria da struct.
func (*Item) Kind() Kind {
	return KindItem
}

// Deleted reflete o bit 3 do campo `info`.
func (i *Item) Deleted() bool {
	return i.Info.Has(ItemFlagDeleted)
}

// Keep reflete o bit 1 do campo `info`.
func (i *Item) Keep() bool {
	return i.Info.Has(ItemFlagKeep)
}

// Countable reflete o bit 2 do campo `info`.
func (i *Item) Countable() bool {
	return i.Info.Has(ItemFlagCountable)
}

// Marker reflete o bit 4 usado como fast-search marker no Yjs.
func (i *Item) Marker() bool {
	return i.Info.Has(ItemFlagMarker)
}

// SetKeep atualiza o bit de retenção para compatibilizar com GC futuro.
func (i *Item) SetKeep(keep bool) {
	i.setFlag(ItemFlagKeep, keep)
}

// SetDeleted atualiza o bit de deleção.
func (i *Item) SetDeleted(deleted bool) {
	i.setFlag(ItemFlagDeleted, deleted)
}

// MarkDeleted é um atalho semântico para o caminho mais comum.
func (i *Item) MarkDeleted() {
	i.Info |= ItemFlagDeleted
}

// SetMarker atualiza o bit de marca rápida.
func (i *Item) SetMarker(marked bool) {
	i.setFlag(ItemFlagMarker, marked)
}

func (i *Item) setFlag(flag ItemFlags, enabled bool) {
	if enabled {
		i.Info |= flag
		return
	}
	i.Info &^= flag
}

func cloneID(id *ID) *ID {
	if id == nil {
		return nil
	}
	copy := *id
	return &copy
}
