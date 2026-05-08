package yhttp

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

const (
	defaultReadLimitBytes    = 16 << 20
	defaultWriteTimeout      = 5 * time.Second
	defaultPersistTimeout    = 5 * time.Second
	defaultFanoutConcurrency = 16
	fanoutParallelThreshold  = 4
)

// Request descreve como uma requisição HTTP deve ser associada ao provider.
//
// `ClientID` precisa corresponder ao client id usado pelo peer Yjs nos payloads
// de awareness; o provider usa esse valor para agregar estado efêmero e gerar
// tombstones no fechamento da conexão.
type Request struct {
	DocumentKey      storage.DocumentKey
	ConnectionID     string
	ClientID         uint32
	PersistOnClose   bool
	SyncOutputFormat yjsbridge.UpdateFormat
	Principal        *Principal
}

// ResolveRequestFunc mapeia a requisição HTTP para o documento e metadados da
// conexão local.
type ResolveRequestFunc func(r *http.Request) (Request, error)

// ErrorHandler recebe erros assíncronos da camada de transporte que já não
// podem mais ser refletidos na resposta HTTP original.
type ErrorHandler func(r *http.Request, req Request, err error)

// Principal descreve a identidade autenticada associada a uma request.
//
// `Tenant` pode ser usado como fronteira multi-tenant contra
// `storage.DocumentKey.Namespace`.
type Principal struct {
	Subject string
	Tenant  string
	Scopes  []string
}

// Authenticator autentica uma request HTTP antes de abrir WebSocket/provider.
type Authenticator interface {
	AuthenticateHTTP(ctx context.Context, r *http.Request) (*Principal, error)
}

// AuthenticatorFunc adapta uma função simples para Authenticator.
type AuthenticatorFunc func(ctx context.Context, r *http.Request) (*Principal, error)

// AuthenticateHTTP autentica a request chamando a função encapsulada.
func (f AuthenticatorFunc) AuthenticateHTTP(ctx context.Context, r *http.Request) (*Principal, error) {
	return f(ctx, r)
}

// Authorizer autoriza uma identidade autenticada para uma request já resolvida.
type Authorizer interface {
	AuthorizeHTTP(ctx context.Context, principal *Principal, req Request) error
}

// AuthorizerFunc adapta uma função simples para Authorizer.
type AuthorizerFunc func(ctx context.Context, principal *Principal, req Request) error

// AuthorizeHTTP autoriza a request chamando a função encapsulada.
func (f AuthorizerFunc) AuthorizeHTTP(ctx context.Context, principal *Principal, req Request) error {
	return f(ctx, principal, req)
}

// RateLimiter limita requests HTTP/WebSocket antes de abrir provider ou
// resolver owner remoto.
type RateLimiter interface {
	AllowHTTP(ctx context.Context, r *http.Request, principal *Principal, req Request) error
}

// RateLimiterFunc adapta uma função simples para RateLimiter.
type RateLimiterFunc func(ctx context.Context, r *http.Request, principal *Principal, req Request) error

// AllowHTTP autoriza a request chamando a função encapsulada.
func (f RateLimiterFunc) AllowHTTP(ctx context.Context, r *http.Request, principal *Principal, req Request) error {
	return f(ctx, r, principal, req)
}

// AuthorityLossHandler assume um websocket ja aceito quando a sessao local
// perde autoridade sobre o documento.
//
// O handler recebe o epoch autoritativo que estava ativo na conexao local e
// passa a ser responsavel por manter ou encerrar o socket do cliente.
type AuthorityLossHandler func(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	previousEpoch uint64,
) error

// ServerConfig define a configuração do handler HTTP/WebSocket.
type ServerConfig struct {
	Provider         *yprotocol.Provider
	OwnershipRuntime *ycluster.DocumentOwnershipRuntime
	ResolveRequest   ResolveRequestFunc
	AcceptOptions    *websocket.AcceptOptions
	ReadLimitBytes   int64
	WriteTimeout     time.Duration
	PersistTimeout   time.Duration
	// FanoutConcurrency limita quantos peers recebem o mesmo broadcast em
	// paralelo. Valores <= 0 usam o padrão do pacote.
	FanoutConcurrency int
	// BootstrapOnConnect envia um bootstrap direto de awareness quando o
	// WebSocket é aceito. É útil para clientes y-websocket receberem presença já
	// conhecida sem aguardar o próximo heartbeat de awareness.
	BootstrapOnConnect bool
	// BootstrapSyncOnConnect inclui um SyncStep1 inicial no bootstrap. Use
	// apenas quando o transporte não iniciar o handshake de sync por conta
	// própria, pois documentos grandes podem gerar payloads e alocações altas.
	BootstrapSyncOnConnect        bool
	AuthorityRevalidationInterval time.Duration
	Authenticator                 Authenticator
	Authorizer                    Authorizer
	RateLimiter                   RateLimiter
	QuotaLimiter                  QuotaLimiter
	OriginPolicy                  OriginPolicy
	Redactor                      RequestRedactor
	Metrics                       Metrics
	OnError                       ErrorHandler
}
