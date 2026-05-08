package binary

// Reader faz leitura sequencial sobre um buffer imutável.
// O offset só avança depois da validação de bounds, para evitar que um parser
// fique em estado inconsistente ao receber input binário malformado.
type Reader struct {
	data   []byte
	offset int
}

// NewReader cria um leitor posicionado no início do buffer.
func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

// NewReaderValue cria um leitor por valor para uso em structs hot-path, evitando
// uma alocação separada quando o caller já possui o armazenamento do Reader.
func NewReaderValue(data []byte) Reader {
	return Reader{data: data}
}

// Offset retorna a posição atual de leitura no buffer.
func (r *Reader) Offset() int {
	return r.offset
}

// Remaining retorna quantos bytes ainda podem ser lidos sem extrapolar o buffer.
func (r *Reader) Remaining() int {
	return len(r.data) - r.offset
}

// ReadByte lê um byte na posição atual.
func (r *Reader) ReadByte() (byte, error) {
	if r.Remaining() < 1 {
		return 0, newUnexpectedEOF("ReadByte", r.offset, 1, r.Remaining())
	}

	b := r.data[r.offset]
	r.offset++
	return b, nil
}

// ReadN lê n bytes sequenciais sem copiar o conteúdo.
// O slice retornado referencia o buffer original para manter a operação barata.
func (r *Reader) ReadN(n int) ([]byte, error) {
	if n < 0 {
		return nil, newInvalidReadSize("ReadN", r.offset, n)
	}

	if r.Remaining() < n {
		return nil, newUnexpectedEOF("ReadN", r.offset, n, r.Remaining())
	}

	start := r.offset
	r.offset += n
	return r.data[start:r.offset], nil
}
