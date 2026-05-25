/* Copyright(C) 2026, homebridge-plugins (https://github.com/homebridge-plugins). All rights reserved.
 *
 * access-code.ts: HomeKit AccessCode TLV helpers.
 */

import { Buffer } from 'node:buffer'

export const ACCESS_CODE_SUPPORTED_CONFIGURATION = buildAccessCodeSupportedConfiguration()

export class AccessCodeProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'AccessCodeProtocolError'
  }
}

export interface AccessCodeRequestRecord {
  accessCode?: string
  identifier?: bigint
}

export interface AccessCodeControlPointRequest {
  operation: number
  records: AccessCodeRequestRecord[]
}

export interface AccessCodeResponseRecord {
  accessCode: string
  flags?: number
  identifier: bigint
  status?: number
}

interface TlvRecord {
  tag: number
  value: Buffer
}

export function parseAccessCodeControlPoint(value: unknown): AccessCodeControlPointRequest {
  const decoded = parseTlv(Buffer.from(String(value), 'base64'))
  const operation = decoded[0]

  if (!operation) {
    throw new AccessCodeProtocolError('Empty AccessCodeControlPoint request')
  }
  if (operation.value.length === 0) {
    throw new AccessCodeProtocolError('AccessCodeControlPoint request is missing its operation')
  }

  return {
    operation: operation.value[0],
    records: decoded.slice(1).map(parseAccessCodeRequestRecord),
  }
}

export function buildAccessCodeOperationResponse(operation: number, records: AccessCodeResponseRecord[]): string {
  const operationRecord = serializeTlv(1, Buffer.from([operation]))
  const responseRecords = records.map(buildAccessCodeResponseRecord)
  const separator = Buffer.from([0, 0])
  const response = responseRecords.length === 0
    ? operationRecord
    : Buffer.concat([
        operationRecord,
        ...responseRecords.flatMap((record, index) => index === 0 ? [record] : [separator, record]),
      ])

  return response.toString('base64')
}

function buildAccessCodeSupportedConfiguration(): string {
  return Buffer.concat([
    serializeTlv(1, Buffer.from([1])),
    serializeTlv(2, Buffer.from([6])),
    serializeTlv(3, Buffer.from([9])),
    serializeTlv(4, Buffer.from([10])),
  ]).toString('base64')
}

function buildAccessCodeResponseRecord(record: AccessCodeResponseRecord): Buffer {
  return serializeTlv(3, Buffer.concat([
    serializeTlv(1, encodeIdentifier(record.identifier)),
    serializeTlv(2, Buffer.from(record.accessCode)),
    serializeTlv(3, Buffer.from([record.flags ?? 0])),
    serializeTlv(4, Buffer.from([record.status ?? 0])),
  ]))
}

function parseAccessCodeRequestRecord(record: TlvRecord): AccessCodeRequestRecord {
  const fields = parseTlv(record.value)
  const identifier = fields.find(field => field.tag === 1)
  const accessCode = fields.find(field => field.tag === 2)

  return {
    accessCode: accessCode?.value.toString(),
    identifier: identifier ? decodeIdentifier(identifier.value) : undefined,
  }
}

function parseTlv(buffer: Buffer): TlvRecord[] {
  const records: TlvRecord[] = []
  let offset = 0

  while (offset < buffer.length) {
    const tag = buffer[offset++]
    const length = buffer[offset++]

    if (tag === undefined || length === undefined) {
      throw new AccessCodeProtocolError('Malformed TLV record')
    }
    if (tag === 0 && length === 0) {
      continue
    }
    if (offset + length > buffer.length) {
      throw new AccessCodeProtocolError('TLV record length exceeds request size')
    }

    records.push({
      tag,
      value: buffer.subarray(offset, offset + length),
    })
    offset += length
  }

  return records
}

function serializeTlv(tag: number, value: Buffer): Buffer {
  if (value.length <= 255) {
    return Buffer.from([tag, value.length, ...value])
  }

  const chunks: Buffer[] = []
  for (let offset = 0; offset < value.length; offset += 255) {
    chunks.push(serializeTlv(tag, value.subarray(offset, offset + 255)))
  }
  return Buffer.concat(chunks)
}

function encodeIdentifier(identifier: bigint): Buffer {
  let hex = identifier.toString(16)
  if (hex.length % 2 !== 0) {
    hex = `0${hex}`
  }
  return Buffer.from(hex, 'hex')
}

function decodeIdentifier(identifier: Buffer): bigint {
  const hex = identifier.toString('hex')
  if (!hex) {
    throw new AccessCodeProtocolError('Access code identifier is empty')
  }
  return BigInt(`0x${hex}`)
}
