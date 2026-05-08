package yupdate

import (
	"context"
	"errors"
	"fmt"
)

// ValidateUpdateFormatWithReason valida um único update preservando a causa detalhada
// da falha de classificação.
func ValidateUpdateFormatWithReason(update []byte) (UpdateFormat, error) {
	return DetectUpdateFormatWithReason(update)
}

// ValidateUpdatesFormatWithReason valida uma lista de updates e garante formato
// único entre eles. O erro retornado mantém contexto da origem do problema.
func ValidateUpdatesFormatWithReason(updates ...[]byte) (UpdateFormat, error) {
	return ValidateUpdatesFormatWithReasonContext(context.Background(), updates...)
}

// ValidateUpdatesFormatWithReasonContext valida uma lista de updates e garante
// formato único entre eles, respeitando cancelamento do contexto.
func ValidateUpdatesFormatWithReasonContext(ctx context.Context, updates ...[]byte) (UpdateFormat, error) {
	return DetectUpdatesFormatWithReasonContext(ctx, updates...)
}

// ValidateUpdate decodifica um único update e retorna erro se o payload não for
// estruturalmente válido para o formato detectado.
func ValidateUpdate(update []byte) error {
	_, err := DecodeUpdate(update)
	return err
}

// ValidateUpdates decodifica múltiplos updates, preservando o índice do payload
// inválido no erro retornado.
func ValidateUpdates(updates ...[]byte) error {
	return ValidateUpdatesContext(context.Background(), updates...)
}

// ValidateUpdatesContext decodifica múltiplos updates respeitando cancelamento
// e preservando o índice do payload inválido no erro retornado.
func ValidateUpdatesContext(ctx context.Context, updates ...[]byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for i, update := range updates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ValidateUpdate(update); err != nil {
			return fmt.Errorf("update[%d]: %w", i, err)
		}
	}
	return nil
}

// FormatFromUpdate identifica o formato do update e retorna erro quando a
// heurística não consegue classificar de forma definitiva.
func FormatFromUpdate(update []byte) (UpdateFormat, error) {
	format, err := DetectUpdateFormatWithReason(update)
	if err != nil {
		if errors.Is(err, ErrAmbiguousUpdateFormat) {
			return UpdateFormatUnknown, ErrUnknownUpdateFormat
		}
		return UpdateFormatUnknown, err
	}
	if format == UpdateFormatUnknown {
		return UpdateFormatUnknown, ErrUnknownUpdateFormat
	}
	return format, nil
}

// FormatFromUpdates identifica o formato comum de uma lista de updates,
// preservando erros indexados e rejeitando payloads vazios na API pública.
func FormatFromUpdates(updates ...[]byte) (UpdateFormat, error) {
	return FormatFromUpdatesContext(context.Background(), updates...)
}

// FormatFromUpdatesContext identifica o formato comum de uma lista de updates,
// preservando erros indexados, rejeitando payloads vazios e respeitando o
// cancelamento do contexto.
func FormatFromUpdatesContext(ctx context.Context, updates ...[]byte) (UpdateFormat, error) {
	for _, update := range updates {
		if len(update) == 0 {
			return UpdateFormatUnknown, ErrUnknownUpdateFormat
		}
	}

	format, err := ValidateUpdatesFormatWithReasonContext(ctx, updates...)
	if err != nil {
		if errors.Is(err, ErrAmbiguousUpdateFormat) {
			return UpdateFormatUnknown, ErrUnknownUpdateFormat
		}
		return UpdateFormatUnknown, err
	}
	if format == UpdateFormatUnknown {
		return UpdateFormatUnknown, ErrUnknownUpdateFormat
	}
	return format, nil
}

// DecodeUpdate trata o update conforme o formato detectado pelo payload.
func DecodeUpdate(update []byte) (*DecodedUpdate, error) {
	format, err := FormatFromUpdate(update)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatV1:
		return DecodeV1(update)
	case UpdateFormatV2:
		return DecodeV2(update)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// StateVectorFromUpdate extrai o state vector de um único update conforme o
// formato detectado no payload.
func StateVectorFromUpdate(update []byte) (map[uint32]uint32, error) {
	format, err := FormatFromUpdate(update)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatV1:
		return stateVectorFromUpdateV1(update)
	case UpdateFormatV2:
		decoded, err := DecodeV2(update)
		if err != nil {
			return nil, err
		}
		return stateVectorFromStructs(decoded.Structs), nil
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// EncodeStateVectorFromUpdate extrai e serializa o state vector de um único
// update conforme o formato detectado no payload.
func EncodeStateVectorFromUpdate(update []byte) ([]byte, error) {
	stateVector, err := StateVectorFromUpdate(update)
	if err != nil {
		return nil, err
	}
	return encodeStateVectorMap(stateVector), nil
}

// CreateContentIDsFromUpdate extrai content ids de um único update conforme o
// formato detectado no payload.
func CreateContentIDsFromUpdate(update []byte) (*ContentIDs, error) {
	format, err := FormatFromUpdate(update)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatV1:
		return CreateContentIDsFromUpdateV1(update)
	case UpdateFormatV2:
		return CreateContentIDsFromUpdateV2(update)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// EncodeUpdate serializa o update em formato V1 canônico.
func EncodeUpdate(update *DecodedUpdate) ([]byte, error) {
	return EncodeV1(update)
}

// EncodeUpdateV2 serializa o update em formato Yjs V2 canônico.
func EncodeUpdateV2(update *DecodedUpdate) ([]byte, error) {
	return EncodeV2(update)
}

// ConvertUpdateToV1 normaliza um update para a representação canônica V1.
//
// Para payloads já em V1, o conteúdo é decodificado e reencodificado de forma
// determinística. Payloads V2 válidos são materializados e emitidos no wire V1.
func ConvertUpdateToV1(update []byte) ([]byte, error) {
	decoded, err := DecodeUpdate(update)
	if err != nil {
		return nil, err
	}
	return EncodeUpdate(decoded)
}

// ConvertUpdateToV2 normaliza um update suportado para a representação V2.
func ConvertUpdateToV2(update []byte) ([]byte, error) {
	decoded, err := DecodeUpdate(update)
	if err != nil {
		return nil, err
	}
	return EncodeUpdateV2(decoded)
}

// ConvertUpdatesToV1 normaliza uma lista de updates para um único payload
// canônico em V1, tratando payloads vazios como no-op.
func ConvertUpdatesToV1(updates ...[]byte) ([]byte, error) {
	return ConvertUpdatesToV1Context(context.Background(), updates...)
}

// ConvertUpdatesToV2 normaliza uma lista de updates para um único payload V2,
// tratando payloads vazios como no-op.
func ConvertUpdatesToV2(updates ...[]byte) ([]byte, error) {
	return ConvertUpdatesToV2Context(context.Background(), updates...)
}

// ConvertUpdatesToV1Context normaliza uma lista de updates para um único
// payload canônico em V1, respeitando cancelamento e tratando payloads vazios
// como no-op.
func ConvertUpdatesToV1Context(ctx context.Context, updates ...[]byte) ([]byte, error) {
	if len(updates) == 0 {
		return MergeUpdatesV1Context(ctx)
	}

	format, err := detectAggregateUpdateFormatSkippingEmptyContext(ctx, updates...)
	if err != nil {
		return nil, err
	}

	switch format {
	case UpdateFormatUnknown:
		return MergeUpdatesV1Context(ctx)
	case UpdateFormatV1:
		filtered := make([][]byte, 0, len(updates))
		for _, update := range updates {
			if len(update) == 0 {
				continue
			}
			filtered = append(filtered, update)
		}
		return MergeUpdatesV1Context(ctx, filtered...)
	case UpdateFormatV2:
		return convertV2UpdatesToV1Context(ctx, updates...)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// ConvertUpdatesToV2Context normaliza uma lista de updates para um único
// payload canônico em V2, respeitando cancelamento e tratando payloads vazios
// como no-op.
func ConvertUpdatesToV2Context(ctx context.Context, updates ...[]byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(updates) == 0 {
		return EncodeUpdateV2(nil)
	}

	format, err := detectAggregateUpdateFormatSkippingEmptyContext(ctx, updates...)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatUnknown:
		return EncodeUpdateV2(nil)
	case UpdateFormatV1, UpdateFormatV2:
	default:
		return nil, ErrUnknownUpdateFormat
	}

	filtered := make([][]byte, 0, len(updates))
	for _, update := range updates {
		if len(update) == 0 {
			continue
		}
		filtered = append(filtered, update)
	}

	merged, err := aggregatePayloadsInParallel(ctx, filtered, 0, decodeMergeUpdate, mergeDecodedUpdatesV1)
	if err != nil {
		return nil, err
	}
	return EncodeUpdateV2(&DecodedUpdate{
		Structs:   merged.blockSet.structs(),
		DeleteSet: merged.deleteSet,
	})
}

// DiffUpdate trata o diff conforme o formato detectado e retorna payload V1.
func DiffUpdate(update, stateVector []byte) ([]byte, error) {
	return DiffUpdateContext(context.Background(), update, stateVector)
}

// DiffUpdateContext trata o diff conforme o formato detectado, retorna payload
// V1 e respeita cancelamento do contexto.
func DiffUpdateContext(ctx context.Context, update, stateVector []byte) ([]byte, error) {
	format, err := FormatFromUpdate(update)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatV1:
		return DiffUpdateV1Context(ctx, update, stateVector)
	case UpdateFormatV2:
		converted, err := ConvertUpdateToV1(update)
		if err != nil {
			return nil, err
		}
		return DiffUpdateV1Context(ctx, converted, stateVector)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// DiffUpdateV2 retorna a parte do update ainda não coberta pelo state vector em V2.
func DiffUpdateV2(update, stateVector []byte) ([]byte, error) {
	return DiffUpdateV2Context(context.Background(), update, stateVector)
}

// DiffUpdateV2Context retorna o diff em V2 respeitando cancelamento.
func DiffUpdateV2Context(ctx context.Context, update, stateVector []byte) ([]byte, error) {
	diffV1, err := DiffUpdateContext(ctx, update, stateVector)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ConvertUpdateToV2(diffV1)
}

// IntersectUpdateWithContentIDs filtra um update mantendo apenas o conteúdo
// selecionado pelos content ids e retorna payload V1.
func IntersectUpdateWithContentIDs(update []byte, contentIDs *ContentIDs) ([]byte, error) {
	return IntersectUpdateWithContentIDsContext(context.Background(), update, contentIDs)
}

// IntersectUpdateWithContentIDsContext filtra um update mantendo apenas o
// conteúdo selecionado pelos content ids, retorna payload V1 e respeita
// cancelamento.
func IntersectUpdateWithContentIDsContext(ctx context.Context, update []byte, contentIDs *ContentIDs) ([]byte, error) {
	format, err := FormatFromUpdate(update)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatV1:
		return IntersectUpdateWithContentIDsV1Context(ctx, update, contentIDs)
	case UpdateFormatV2:
		converted, err := ConvertUpdateToV1(update)
		if err != nil {
			return nil, err
		}
		return IntersectUpdateWithContentIDsV1Context(ctx, converted, contentIDs)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// IntersectUpdateWithContentIDsV2 filtra um update mantendo apenas o conteúdo
// selecionado pelos content ids e retorna payload V2.
func IntersectUpdateWithContentIDsV2(update []byte, contentIDs *ContentIDs) ([]byte, error) {
	return IntersectUpdateWithContentIDsV2Context(context.Background(), update, contentIDs)
}

// IntersectUpdateWithContentIDsV2Context filtra um update em V2 respeitando cancelamento.
func IntersectUpdateWithContentIDsV2Context(ctx context.Context, update []byte, contentIDs *ContentIDs) ([]byte, error) {
	intersectedV1, err := IntersectUpdateWithContentIDsContext(ctx, update, contentIDs)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ConvertUpdateToV2(intersectedV1)
}

// MergeUpdates consolida múltiplos updates em um payload V1 e valida
// consistência de formato.
// Quando há mistura de formatos, retorna erro explícito.
func MergeUpdates(updates ...[]byte) ([]byte, error) {
	return MergeUpdatesContext(context.Background(), updates...)
}

// MergeUpdatesContext consolida múltiplos updates em um payload V1, valida
// consistência de formato e respeita cancelamento do contexto na etapa de fusão.
func MergeUpdatesContext(ctx context.Context, updates ...[]byte) ([]byte, error) {
	if len(updates) == 0 {
		return MergeUpdatesV1Context(ctx)
	}
	allEmpty := true
	for _, update := range updates {
		if len(update) != 0 {
			allEmpty = false
			break
		}
	}
	if allEmpty {
		return MergeUpdatesV1Context(ctx)
	}

	format, err := FormatFromUpdatesContext(ctx, updates...)
	if err != nil {
		return nil, err
	}

	switch format {
	case UpdateFormatV1:
		return MergeUpdatesV1Context(ctx, updates...)
	case UpdateFormatV2:
		return convertV2UpdatesToV1Context(ctx, updates...)
	default:
		return nil, ErrUnknownUpdateFormat
	}
}

// MergeUpdatesV2 consolida múltiplos updates em um payload V2 e valida
// consistência de formato entre payloads não vazios.
func MergeUpdatesV2(updates ...[]byte) ([]byte, error) {
	return MergeUpdatesV2Context(context.Background(), updates...)
}

// MergeUpdatesV2Context consolida múltiplos updates em V2 respeitando cancelamento.
func MergeUpdatesV2Context(ctx context.Context, updates ...[]byte) ([]byte, error) {
	if len(updates) == 0 {
		return ConvertUpdatesToV2Context(ctx)
	}
	return ConvertUpdatesToV2Context(ctx, updates...)
}

func convertV2UpdatesToV1Context(ctx context.Context, updates ...[]byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	filtered := make([][]byte, 0, len(updates))
	for _, update := range updates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(update) == 0 {
			continue
		}
		filtered = append(filtered, update)
	}

	merged, err := aggregatePayloadsInParallel(ctx, filtered, 0, decodeMergeUpdate, mergeDecodedUpdatesV1)
	if err != nil {
		return nil, err
	}
	out, err := encodeStructGroupsV1(merged.blockSet.clients)
	if err != nil {
		return nil, err
	}
	return AppendDeleteSetBlockV1(out, merged.deleteSet), nil
}
