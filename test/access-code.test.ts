import { Buffer } from 'node:buffer'

import { describe, expect, it } from 'vitest'

import {
  ACCESS_CODE_SUPPORTED_CONFIGURATION,
  buildAccessCodeOperationResponse,
  parseAccessCodeControlPoint,
} from '../src/devices/access-code.js'

describe('accessCode TLV helpers', () => {
  it('uses the TTLock-tested supported configuration TLV', () => {
    expect(ACCESS_CODE_SUPPORTED_CONFIGURATION).toBe('AQEBAgEGAwEJBAEK')
  })

  it('parses add requests with an access code payload', () => {
    const request = Buffer.from('01010303080206313233343536', 'hex').toString('base64')

    expect(parseAccessCodeControlPoint(request)).toEqual({
      operation: 3,
      records: [
        {
          accessCode: '123456',
          identifier: undefined,
        },
      ],
    })
  })

  it('parses read requests with an identifier payload', () => {
    const request = Buffer.from('0101020303010107', 'hex').toString('base64')

    expect(parseAccessCodeControlPoint(request)).toEqual({
      operation: 2,
      records: [
        {
          accessCode: undefined,
          identifier: 7n,
        },
      ],
    })
  })

  it('builds list/read/add/delete response records', () => {
    const response = buildAccessCodeOperationResponse(1, [
      {
        accessCode: '123456',
        identifier: 7n,
      },
    ])

    expect(Buffer.from(response, 'base64').toString('hex')).toBe('01010103110101070206313233343536030100040100')
  })
})
