import type { CharacteristicValue, PlatformAccessory, Service } from 'homebridge'

import type { AugustPlatform } from '../Platform.HAP.js'
import type { device, devicesConfig, KeypadInfo, lockDetails, lockEvent, lockStatus } from '../settings.js'

/* Copyright(C) 2021-2024, donavanbecker (https://github.com/donavanbecker). All rights reserved.
 *
 * lock.ts: homebridge-august.
 */
import August from 'august-yale'
import { Subject } from 'rxjs'
import { debounceTime, tap } from 'rxjs/operators'

import {
  ACCESS_CODE_SUPPORTED_CONFIGURATION,
  AccessCodeProtocolError,
  buildAccessCodeOperationResponse,
  parseAccessCodeControlPoint,
} from './access-code.js'
import { deviceBase } from './device.js'

interface AugustAccessCodeRecord {
  accessType?: string
  firstName?: string
  lastName?: string
  pin: string
  raw?: unknown
  slot: number
  state?: string
  userId: string
}

interface AccessCodeContextRecord {
  accessType?: string
  firstName?: string
  lastName?: string
  source: 'august'
  slot: number
  userId: string
}

interface AccessCodeContext {
  byHomeKitIdentifier: Record<string, AccessCodeContextRecord>
  byUserId: Record<string, number>
  nextIdentifier: number
}

interface PendingAccessCodeDelete {
  identifier: bigint
  record: AugustAccessCodeRecord
  requestedAt: number
  timer: NodeJS.Timeout
}

interface BatteryServiceState {
  Name: CharacteristicValue
  Service: Service
  BatteryLevel: CharacteristicValue
  StatusLowBattery: CharacteristicValue
  ChargingState: CharacteristicValue
}

const ACCESS_CODE_DELETE_REPLACE_WINDOW_MS = 10_000

/**
 * Platform Accessory
 * An instance of this class is created for each accessory your platform registers
 * Each accessory may expose multiple services of different service types.
 */
export class LockMechanism extends deviceBase {
  // Service
  private LockMechanism?: {
    Name: CharacteristicValue
    Service: Service
    LockTargetState: CharacteristicValue
    LockCurrentState: CharacteristicValue
  }

  private Battery: BatteryServiceState

  private ContactSensor?: {
    Name: CharacteristicValue
    Service: Service
    ContactSensorState: CharacteristicValue
  }

  private AccessCode?: {
    ConfigurationState: CharacteristicValue
    Name: CharacteristicValue
    Service: Service
  }

  private accessCodeBusy = false
  private accessCodeOperationQueue: Promise<unknown> = Promise.resolve()
  private accessCodePendingOperations = 0
  private pendingAccessCodeDeletes = new Map<string, PendingAccessCodeDelete>()
  private accessCodeRequestPromises = new Map<string, Promise<string>>()

  // Lock Mechanism
  lockEvent!: lockEvent
  lockStatus!: lockStatus
  lockDetails!: lockDetails

  // Lock Updates
  lockUpdateInProgress: boolean
  doLockUpdate: any

  // PubNub subscription cleanup function. Captured from August.subscribe()
  // so the subscription can be properly torn down (both when the lock is
  // unregistered and before creating a replacement subscription).
  private pubnubUnsubscribe?: () => void

  constructor(
    readonly platform: AugustPlatform,
    accessory: PlatformAccessory,
    device: device & devicesConfig,
  ) {
    super(platform, accessory, device)

    // this is subject we use to track when we need to POST changes to the August API
    this.doLockUpdate = new Subject()
    this.lockUpdateInProgress = false

    // Initialize Lock Mechanism Service
    if (device.lock?.hide_lock) {
      if (this.LockMechanism?.Service) {
        this.debugLog('Removing Lock Mechanism Service')
        this.LockMechanism.Service = accessory.getService(this.hap.Service.LockMechanism) as Service
        accessory.removeService(this.LockMechanism.Service)
        accessory.context.LockMechanism = {}
      }
    } else {
      accessory.context.LockMechanism = accessory.context.LockMechanism ?? {}
      this.LockMechanism = {
        Name: accessory.displayName,
        Service: accessory.getService(this.hap.Service.LockMechanism) ?? accessory.addService(this.hap.Service.LockMechanism) as Service,
        LockTargetState: accessory.context.LockMechanismLockTargetState ?? this.hap.Characteristic.LockTargetState.SECURED,
        LockCurrentState: accessory.context.LockMechanismLockCurrentState ?? this.hap.Characteristic.LockCurrentState.SECURED,
      }
      accessory.context.LockMechanism = this.LockMechanism as object
      // Seed context keys for updateCharacteristic change detection
      accessory.context.LockMechanismLockCurrentState ??= this.LockMechanism.LockCurrentState
      accessory.context.LockMechanismLockTargetState ??= this.LockMechanism.LockTargetState
      // Initialize Lock Mechanism Characteristics
      this.LockMechanism.Service
        .setCharacteristic(this.hap.Characteristic.Name, this.LockMechanism.Name)
        .getCharacteristic(this.hap.Characteristic.LockTargetState)
        .onGet(() => {
          return this.LockMechanism!.LockTargetState
        })
        .onSet(this.setLockTargetState.bind(this))
      this.LockMechanism.Service
        .getCharacteristic(this.hap.Characteristic.LockCurrentState)
        .onGet(() => {
          return this.LockMechanism!.LockCurrentState
        })
    }
    // Initialize Contact Sensor Service
    if (device.lock?.hide_contactsensor) {
      if (this.ContactSensor?.Service) {
        this.debugLog('Removing Conact Sensor Service')
        this.ContactSensor.Service = accessory.getService(this.hap.Service.ContactSensor) as Service
        accessory.removeService(this.ContactSensor.Service)
        accessory.context.ContactSensor = {}
      }
    } else {
      accessory.context.ContactSensor = accessory.context.ContactSensor ?? {}
      this.ContactSensor = {
        Name: `${accessory.displayName} Contact Sensor`,
        Service: accessory.getService(this.hap.Service.ContactSensor) ?? accessory.addService(this.hap.Service.ContactSensor) as Service,
        ContactSensorState: accessory.context.ContactSensorContactSensorState ?? this.hap.Characteristic.ContactSensorState.CONTACT_DETECTED,
      }
      accessory.context.ContactSensor = this.ContactSensor as object
      // Seed context key for updateCharacteristic change detection
      accessory.context.ContactSensorContactSensorState ??= this.ContactSensor.ContactSensorState
      // Initialize Conact Sensor Characteristics
      this.ContactSensor.Service
        .setCharacteristic(this.hap.Characteristic.Name, this.ContactSensor.Name)
        .getCharacteristic(this.hap.Characteristic.ContactSensorState)
        .onGet(() => {
          return this.ContactSensor!.ContactSensorState
        })
    }

    this.configureAccessCodeService(accessory, device)

    // Initialize Battery Service. Reuse the legacy untyped Battery service
    // for cached accessories, then link it to LockMechanism when present.
    // Home app renders one lock tile, so this service reports the lowest
    // battery state across the lock and keypad.
    accessory.context.Battery = accessory.context.Battery ?? {}
    const legacyBatteryService = accessory.getService(this.hap.Service.Battery)
    this.Battery = {
      Name: `${accessory.displayName} Battery`,
      Service: this.getBatteryService(accessory, `${accessory.displayName} Battery`, 'lock-battery', legacyBatteryService),
      BatteryLevel: accessory.context.BatteryBatteryLevel ?? 100,
      StatusLowBattery: accessory.context.BatteryStatusLowBattery ?? this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_NORMAL,
      ChargingState: accessory.context.ChargingState ?? this.hap.Characteristic.ChargingState.NOT_CHARGING,
    }
    accessory.context.Battery = this.Battery as object
    // Seed context keys for updateCharacteristic change detection
    accessory.context.BatteryBatteryLevel ??= this.Battery.BatteryLevel
    accessory.context.BatteryStatusLowBattery ??= this.Battery.StatusLowBattery
    // Initialize Battery Characteristics
    this.Battery.Service
      .setCharacteristic(this.hap.Characteristic.Name, this.Battery.Name)
      .setCharacteristic(this.hap.Characteristic.ChargingState, this.hap.Characteristic.ChargingState.NOT_CHARGEABLE)
      .getCharacteristic(this.hap.Characteristic.BatteryLevel)
      .onGet(() => {
        return this.Battery.BatteryLevel
      })

    this.Battery.Service
      .getCharacteristic(this.hap.Characteristic.StatusLowBattery)
      .onGet(() => {
        return this.Battery.StatusLowBattery
      })

    this.linkService(this.LockMechanism?.Service, this.Battery.Service)
    this.removeLegacyKeypadBatteryService(accessory)

    // Initial Device Refresh
    this.refreshStatus()

    // Subscribe to august changes. PubNub subscriptions are independent of
    // the HTTP session and survive refreshAugustSession() — no resubscribe
    // needed. The August.subscribe() call uses its own internal August
    // instance dedicated to PubNub, separate from platform.augustConfig.
    this.subscribeAugust()

    // Polling is now owned by the platform. AugustPlatform.startPolling()
    // iterates registered locks serially and short-circuits on the first
    // failure, so a network outage produces ONE timeout per cycle instead
    // of N (one per lock). The previous per-lock rxjs interval that lived
    // here was the source of the log-spam cascade after router restarts.

    // Watch for Lock change events
    // We put in a debounce of 100ms so we don't make duplicate calls
    if (!device.lock?.hide_lock) {
      this.doLockUpdate
        .pipe(
          tap(() => {
            this.lockUpdateInProgress = true
          }),
          debounceTime(this.devicePushRate * 1000),
        )
        .subscribe(async () => {
          try {
            await this.pushChanges()
          } catch (e: any) {
            await this.statusCode('pushChanges', e)
            await this.errorLog(`doLockUpdate pushChanges: ${e.message ?? e}`)
          }
          this.lockUpdateInProgress = false
        })
    }
  }

  private getBatteryService(accessory: PlatformAccessory, name: string, subtype: string, legacyService?: Service): Service {
    const existingService = (accessory as any).getServiceById?.(this.hap.Service.Battery, subtype) as Service | undefined
    return existingService ?? legacyService ?? accessory.addService(this.hap.Service.Battery, name, subtype) as Service
  }

  private removeLegacyKeypadBatteryService(accessory: PlatformAccessory): void {
    const existingService = (accessory as any).getServiceById?.(this.hap.Service.Battery, 'keypad-battery') as Service | undefined

    if (existingService) {
      accessory.removeService(existingService)
    }
    accessory.context.KeypadBattery = {}
    delete accessory.context.KeypadBatteryBatteryLevel
    delete accessory.context.KeypadBatteryStatusLowBattery
    delete accessory.context.KeypadBatteryChargingState
  }

  private linkService(primary: Service | undefined, linked: Service | undefined): void {
    if (!primary || !linked) {
      return
    }

    const linkedServices = (primary as any).linkedServices as Service[] | undefined
    if (linkedServices?.includes(linked)) {
      return
    }

    primary.addLinkedService(linked)
  }

  private configureAccessCodeService(accessory: PlatformAccessory, device: device & devicesConfig): void {
    const AccessCodeService = (this.hap.Service as any).AccessCode
    const AccessCodeSupportedConfiguration = (this.hap.Characteristic as any).AccessCodeSupportedConfiguration
    const AccessCodeControlPoint = (this.hap.Characteristic as any).AccessCodeControlPoint
    const ConfigurationState = (this.hap.Characteristic as any).ConfigurationState
    const shouldExposeAccessCode = device.supportsEntryCodes && !device.lock?.hide_accesscode

    if (!AccessCodeService || !AccessCodeSupportedConfiguration || !AccessCodeControlPoint || !ConfigurationState) {
      this.debugWarnLog('Homebridge does not expose the AccessCode service or characteristics; skipping AccessCode service')
      return
    }

    const existingService = accessory.getService(AccessCodeService)
    if (!shouldExposeAccessCode) {
      if (existingService) {
        this.debugLog('Removing AccessCode Service')
        accessory.removeService(existingService)
        accessory.context.AccessCode = {}
      }
      return
    }

    accessory.context.AccessCode = accessory.context.AccessCode ?? {}
    this.AccessCode = {
      Name: `${accessory.displayName} Access Codes`,
      Service: existingService ?? accessory.addService(AccessCodeService, `${accessory.displayName} Access Codes`, AccessCodeService.UUID) as Service,
      ConfigurationState: accessory.context.AccessCodeConfigurationState ?? 1,
    }
    accessory.context.AccessCode = this.AccessCode as object
    accessory.context.AccessCodeConfigurationState ??= this.AccessCode.ConfigurationState
    this.getAccessCodeContext()

    this.AccessCode.Service
      .setCharacteristic(this.hap.Characteristic.Name, this.AccessCode.Name)
      .getCharacteristic(AccessCodeSupportedConfiguration)
      .onGet(() => ACCESS_CODE_SUPPORTED_CONFIGURATION)
      .updateValue(ACCESS_CODE_SUPPORTED_CONFIGURATION)

    this.AccessCode.Service
      .getCharacteristic(AccessCodeControlPoint)
      .onGet(() => '')
      .onSet(async value => await this.setAccessCodeControlPoint(value))
      .updateValue('')

    this.AccessCode.Service
      .getCharacteristic(ConfigurationState)
      .onGet(() => this.getAccessCodeConfigurationState())
      .updateValue(this.getAccessCodeConfigurationState())
  }

  private getAccessCodeContext(): AccessCodeContext {
    const context = this.accessory.context as Record<string, any>
    const existing = context.accessCodes

    if (!existing || typeof existing !== 'object') {
      context.accessCodes = {
        byHomeKitIdentifier: {},
        byUserId: {},
        nextIdentifier: 1,
      } satisfies AccessCodeContext
      return context.accessCodes
    }

    existing.byHomeKitIdentifier ??= {}
    existing.byUserId ??= {}
    existing.nextIdentifier = Number.isFinite(Number(existing.nextIdentifier))
      ? Number(existing.nextIdentifier)
      : 1

    return existing as AccessCodeContext
  }

  private setAccessCodeBusy(busy: boolean): void {
    this.accessCodeBusy = busy
    this.accessory.context.AccessCodeConfigurationState = this.getAccessCodeConfigurationState()

    const ConfigurationState = (this.hap.Characteristic as any).ConfigurationState
    if (this.AccessCode?.Service && ConfigurationState) {
      this.AccessCode.Service
        .getCharacteristic(ConfigurationState)
        .updateValue(this.getAccessCodeConfigurationState())
    }
  }

  private getAccessCodeConfigurationState(): number {
    return this.accessCodeBusy ? 0 : 1
  }

  private clampBatteryLevel(value: unknown, fallback = 100): number {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) {
      return Math.min(Math.max(Math.round(fallback), 0), 100)
    }

    return Math.min(Math.max(Math.round(numeric), 0), 100)
  }

  private lockBatteryLevel(lockDetails: lockDetails): number {
    const level = lockDetails.batteryInfo?.level ?? lockDetails.battery
    const numeric = Number(level)
    if (!Number.isFinite(numeric)) {
      return this.clampBatteryLevel(this.Battery.BatteryLevel)
    }

    return this.clampBatteryLevel(numeric <= 1 ? numeric * 100 : numeric)
  }

  private keypadBatteryLevel(keypad: KeypadInfo | undefined): number | undefined {
    switch (this.normalizedKeypadBatteryLevel(keypad)) {
      case 'full':
        return 100
      case 'medium':
        return 50
      case 'low':
        return 20
      case 'very low':
        return 5
      default:
        return undefined
    }
  }

  private keypadStatusLowBattery(keypad: KeypadInfo | undefined, batteryLevel: number): CharacteristicValue {
    const level = this.normalizedKeypadBatteryLevel(keypad)
    return level === 'low' || level === 'very low' || batteryLevel < 15
      ? this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_LOW
      : this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_NORMAL
  }

  private normalizedKeypadBatteryLevel(keypad: KeypadInfo | undefined): string {
    return String(keypad?.batteryLevel ?? '').trim().toLowerCase()
  }

  private async fetchAccessCodeRecords(): Promise<AugustAccessCodeRecord[]> {
    if (!this.platform.connectivity) {
      throw new Error('accessCode: connectivity not initialized')
    }

    const response = await this.platform.connectivity.execute(
      `accessCode pins ${this.device.lockId}`,
      async client => await (client as any).pins(this.device.lockId),
      { throwOnOffline: true },
    )

    if (response === undefined) {
      throw new Error(`No PIN records returned for lock ${this.device.lockId}`)
    }

    return this.filterPendingAccessCodeRecords(this.normalizeAccessCodeRecords(response))
  }

  private async fetchAccessCodeStateRecords(): Promise<AugustAccessCodeRecord[]> {
    if (!this.platform.connectivity) {
      throw new Error('accessCode: connectivity not initialized')
    }

    const response = await this.platform.connectivity.execute(
      `accessCode pinStates ${this.device.lockId}`,
      async (client) => {
        const august = client as any
        return typeof august.pinStates === 'function'
          ? await august.pinStates(this.device.lockId)
          : await august.pins(this.device.lockId)
      },
      { throwOnOffline: true },
    )

    if (response === undefined) {
      throw new Error(`No PIN state records returned for lock ${this.device.lockId}`)
    }

    return this.filterPendingAccessCodeRecords(this.normalizeAccessCodeRecords(response, true))
  }

  private filterPendingAccessCodeRecords(records: AugustAccessCodeRecord[]): AugustAccessCodeRecord[] {
    if (this.pendingAccessCodeDeletes.size === 0) {
      return records
    }

    const pendingUserIds = new Set([...this.pendingAccessCodeDeletes.values()].map(deleteRequest => deleteRequest.record.userId))
    return records.filter(record => !pendingUserIds.has(record.userId))
  }

  private normalizeAccessCodeRecords(response: unknown, includeAllStates = false): AugustAccessCodeRecord[] {
    const body = (response as any)?.body ?? response
    const records = includeAllStates
      ? this.accessCodeRecordsForAllStates(body)
      : this.loadedAccessCodeRecords(body)

    return records
      .map(({ record, state }): AugustAccessCodeRecord | undefined => {
        const pin = this.firstString(record, ['pin', 'Pin', 'PIN'])
        const userId = this.firstString(record, ['userID', 'userId', 'UserID', 'UserId', 'id'])
        const slot = Number(this.firstValue(record, ['slot', 'Slot']))

        if (!pin || !userId || !Number.isFinite(slot)) {
          return undefined
        }

        return {
          accessType: this.firstString(record, ['accessType', 'AccessType']),
          firstName: this.firstString(record, ['firstName', 'FirstName']),
          lastName: this.firstString(record, ['lastName', 'LastName']),
          pin,
          raw: record,
          slot,
          state: this.firstString(record, ['state', 'State']) ?? state,
          userId,
        } satisfies AugustAccessCodeRecord
      })
      .filter((record: AugustAccessCodeRecord | undefined): record is AugustAccessCodeRecord => Boolean(record))
  }

  private loadedAccessCodeRecords(body: any): Array<{ record: any, state: string }> {
    if (Array.isArray(body?.loaded)) {
      return body.loaded.map((record: any) => ({ record, state: 'loaded' }))
    }

    if (Array.isArray(body)) {
      return body.map(record => ({ record, state: this.firstString(record, ['state', 'State']) ?? 'loaded' }))
    }

    if (body) {
      return [{ record: body, state: this.firstString(body, ['state', 'State']) ?? 'loaded' }]
    }

    return []
  }

  private accessCodeRecordsForAllStates(body: any): Array<{ record: any, state: string }> {
    const states = ['loaded', 'created', 'disabled', 'disabling', 'enabling', 'deleting', 'updating']
    const records: Array<{ record: any, state: string }> = []

    if (body && typeof body === 'object' && !Array.isArray(body)) {
      for (const state of states) {
        const stateRecords = body[state]
        if (Array.isArray(stateRecords)) {
          records.push(...stateRecords.map((record: any) => ({ record, state })))
        }
      }

      if (records.length > 0) {
        return records
      }
    }

    return this.loadedAccessCodeRecords(body)
  }

  private syncAccessCodeContext(records: AugustAccessCodeRecord[]): AccessCodeContext {
    const context = this.getAccessCodeContext()
    const nextByHomeKitIdentifier: Record<string, AccessCodeContextRecord> = {}
    const nextByUserId: Record<string, number> = {}
    const existingIdentifiers = Object.keys(context.byHomeKitIdentifier)
      .map(identifier => Number(identifier))
      .filter(identifier => Number.isFinite(identifier) && identifier > 0)
    let nextIdentifier = Math.max(context.nextIdentifier, ...existingIdentifiers, 0) || 1

    for (const record of records) {
      let identifier = context.byUserId[record.userId]

      if (!identifier || identifier <= 0) {
        identifier = nextIdentifier++
      }

      nextByUserId[record.userId] = identifier
      nextByHomeKitIdentifier[String(identifier)] = {
        accessType: record.accessType,
        firstName: record.firstName,
        lastName: record.lastName,
        source: 'august',
        slot: record.slot,
        userId: record.userId,
      }
    }

    for (const pendingDelete of this.pendingAccessCodeDeletes.values()) {
      const identifier = Number(pendingDelete.identifier)
      if (!Number.isFinite(identifier) || identifier <= 0) {
        continue
      }

      nextByUserId[pendingDelete.record.userId] = identifier
      nextByHomeKitIdentifier[String(identifier)] = {
        accessType: pendingDelete.record.accessType,
        firstName: pendingDelete.record.firstName,
        lastName: pendingDelete.record.lastName,
        source: 'august',
        slot: pendingDelete.record.slot,
        userId: pendingDelete.record.userId,
      }
      nextIdentifier = Math.max(nextIdentifier, identifier + 1)
    }

    context.byHomeKitIdentifier = nextByHomeKitIdentifier
    context.byUserId = nextByUserId
    context.nextIdentifier = records.length === 0 && this.pendingAccessCodeDeletes.size === 0 ? 1 : Math.max(nextIdentifier, 1)
    this.api.updatePlatformAccessories([this.accessory])

    return context
  }

  private getHomeKitIdentifier(record: AugustAccessCodeRecord, context: AccessCodeContext): bigint {
    const identifier = context.byUserId[record.userId]
    if (!identifier) {
      throw new AccessCodeProtocolError(`Access code mapping for user ${record.userId} was not found`)
    }

    return BigInt(identifier)
  }

  private findRecordByHomeKitIdentifier(records: AugustAccessCodeRecord[], context: AccessCodeContext, identifier: bigint): AugustAccessCodeRecord | undefined {
    const mapped = context.byHomeKitIdentifier[identifier.toString()]
    if (!mapped) {
      return undefined
    }

    return records.find(record => record.userId === mapped.userId)
  }

  private buildAccessCodeRecord(record: AugustAccessCodeRecord, context: AccessCodeContext) {
    return {
      accessCode: record.pin,
      identifier: this.getHomeKitIdentifier(record, context),
    }
  }

  private accessCodeIdentifierKey(identifier: bigint): string {
    return identifier.toString()
  }

  private getPendingAccessCodeDelete(identifier: bigint): PendingAccessCodeDelete | undefined {
    return this.pendingAccessCodeDeletes.get(this.accessCodeIdentifierKey(identifier))
  }

  private restoreAccessCodeMapping(identifier: bigint, record: AugustAccessCodeRecord): void {
    const context = this.getAccessCodeContext()
    const numericIdentifier = Number(identifier)
    if (!Number.isFinite(numericIdentifier) || numericIdentifier <= 0) {
      return
    }

    context.byUserId[record.userId] = numericIdentifier
    context.byHomeKitIdentifier[this.accessCodeIdentifierKey(identifier)] = {
      accessType: record.accessType,
      firstName: record.firstName,
      lastName: record.lastName,
      source: 'august',
      slot: record.slot,
      userId: record.userId,
    }
    context.nextIdentifier = Math.max(context.nextIdentifier, numericIdentifier + 1)
  }

  private schedulePendingAccessCodeDelete(identifier: bigint, record: AugustAccessCodeRecord): void {
    const key = this.accessCodeIdentifierKey(identifier)
    const existing = this.pendingAccessCodeDeletes.get(key)
    if (existing) {
      clearTimeout(existing.timer)
    }

    const timer = setTimeout(() => {
      void this.enqueueAccessCodeOperation(async () => {
        await this.finalizePendingAccessCodeDelete(key)
      }).catch(async (error) => {
        await this.errorLog(`AccessCode pending delete failed on ${this.accessory.displayName}: ${error?.message ?? error}`)
      })
    }, ACCESS_CODE_DELETE_REPLACE_WINDOW_MS)
    timer.unref?.()

    this.pendingAccessCodeDeletes.set(key, {
      identifier,
      record,
      requestedAt: Date.now(),
      timer,
    })
  }

  private consumePendingAccessCodeDeleteForAdd(): PendingAccessCodeDelete | undefined {
    const pendingDelete = [...this.pendingAccessCodeDeletes.values()]
      .sort((a, b) => a.requestedAt - b.requestedAt)[0]
    if (!pendingDelete) {
      return undefined
    }

    clearTimeout(pendingDelete.timer)
    this.pendingAccessCodeDeletes.delete(this.accessCodeIdentifierKey(pendingDelete.identifier))
    this.restoreAccessCodeMapping(pendingDelete.identifier, pendingDelete.record)

    return pendingDelete
  }

  private async finalizePendingAccessCodeDelete(identifierKey: string): Promise<void> {
    const pendingDelete = this.pendingAccessCodeDeletes.get(identifierKey)
    if (!pendingDelete) {
      return
    }

    this.pendingAccessCodeDeletes.delete(identifierKey)
    await this.debugLog(`Finalizing pending AccessCode delete identifier ${pendingDelete.identifier.toString()} on ${this.accessory.displayName}`)
    const deleteResult = await this.runAccessCodeApiOperation('deletePin', async client => await (client as any).deletePin(this.device.lockId, pendingDelete.record.pin))
    await this.logPinOperationResult('Delete API result', deleteResult)
    const records = await this.fetchAccessCodeRecords()
    this.syncAccessCodeContext(records)
  }

  private accessCodeOperationName(operation: number): string {
    switch (operation) {
      case 1:
        return 'List'
      case 2:
        return 'Read'
      case 3:
        return 'Add'
      case 5:
        return 'Delete'
      default:
        return `Unknown(${operation})`
    }
  }

  private accessCodeRequestKey(request: ReturnType<typeof parseAccessCodeControlPoint>): string {
    const records = request.records
      .map(record => `${this.accessCodeIdentifierForLog(record.identifier)}:${record.accessCode ?? ''}`)
      .join('|')

    return `${request.operation}:${records}`
  }

  private accessCodeForLog(pin?: string): string {
    if (!pin) {
      return '(none)'
    }

    if (this.deviceLogging === 'debug' || this.deviceLogging === 'debugMode') {
      return pin
    }

    if (pin.length <= 2) {
      return '*'.repeat(pin.length)
    }

    return `${'*'.repeat(pin.length - 2)}${pin.slice(-2)}`
  }

  private accessCodeIdentifierForLog(identifier?: bigint): string {
    return identifier === undefined ? '(none)' : identifier.toString()
  }

  private accessCodeUserIdForLog(userId?: string): string {
    return userId ? `${userId.slice(0, 8)}...` : '(none)'
  }

  private async logHomeKitAccessCodeRequest(requestType: string, records: Array<{ accessCode?: string, identifier?: bigint }>): Promise<void> {
    const summary = records.length === 0
      ? 'records=0'
      : records
          .map(record => `identifier=${this.accessCodeIdentifierForLog(record.identifier)} code=${this.accessCodeForLog(record.accessCode)}`)
          .join('; ')

    await this.infoLog(`AccessCode ${requestType} HomeKit request: ${summary}`)
  }

  private async logAugustAccessCodeRecords(label: string, records: AugustAccessCodeRecord[]): Promise<void> {
    const summary = records.length === 0
      ? 'none'
      : records
          .map((record) => {
            const name = [record.firstName, record.lastName].filter(Boolean).join(' ').trim() || '(unnamed)'
            return `state=${record.state ?? 'unknown'} slot=${record.slot} user=${this.accessCodeUserIdForLog(record.userId)} code=${this.accessCodeForLog(record.pin)} name="${name}"`
          })
          .join('; ')

    await this.infoLog(`AccessCode ${label}: ${summary}`)
  }

  private async logPinOperationResult(label: string, result: any): Promise<void> {
    const summary = [
      `slot=${result?.slot ?? '(none)'}`,
      `user=${this.accessCodeUserIdForLog(result?.userId)}`,
      `pin=${this.accessCodeForLog(result?.pin)}`,
      `previous=${this.accessCodeForLog(result?.previousPin)}`,
      `generated=${this.accessCodeForLog(result?.generatedPin)}`,
      result?.rollback ? `rollback=${JSON.stringify(result.rollback)}` : undefined,
    ].filter(Boolean).join(' ')

    await this.infoLog(`AccessCode ${label}: ${summary}`)
  }

  private enqueueAccessCodeOperation<T>(action: () => Promise<T>): Promise<T> {
    this.accessCodePendingOperations += 1
    this.setAccessCodeBusy(true)

    const run = this.accessCodeOperationQueue
      .catch(() => undefined)
      .then(async () => {
        try {
          return await action()
        } finally {
          this.accessCodePendingOperations = Math.max(0, this.accessCodePendingOperations - 1)
          this.setAccessCodeBusy(this.accessCodePendingOperations > 0)
        }
      })

    this.accessCodeOperationQueue = run.catch(() => undefined)

    return run
  }

  private async buildAccessCodeListResponse(): Promise<string> {
    const records = await this.fetchAccessCodeRecords()
    const context = this.syncAccessCodeContext(records)

    return buildAccessCodeOperationResponse(1, records.map(record => this.buildAccessCodeRecord(record, context)))
  }

  private async buildAccessCodeReadResponse(identifiers: bigint[]): Promise<string> {
    const records = await this.fetchAccessCodeRecords()
    const context = this.syncAccessCodeContext(records)
    const responseRecords: ReturnType<typeof this.buildAccessCodeRecord>[] = []
    for (const identifier of identifiers) {
      const record = this.findRecordByHomeKitIdentifier(records, context, identifier)
      if (!record) {
        if (this.getPendingAccessCodeDelete(identifier)) {
          await this.debugLog(`AccessCode identifier ${identifier.toString()} is pending delete on ${this.accessory.displayName}; omitting it from read response`)
          continue
        }
        throw new AccessCodeProtocolError(`Access code identifier ${identifier.toString()} was not found for read`)
      }

      responseRecords.push(this.buildAccessCodeRecord(record, context))
    }

    return buildAccessCodeOperationResponse(2, responseRecords)
  }

  private async buildAccessCodeAddResponse(accessCodes: Array<{ accessCode: string, identifier?: bigint }>): Promise<string> {
    const responseRecords: Array<ReturnType<typeof this.buildAccessCodeRecord> | undefined> = Array.from({ length: accessCodes.length })
    const pendingAddCodes: string[] = []
    const pendingAddIndexes = new Map<string, number[]>()

    for (const request of accessCodes) {
      if (!request.accessCode) {
        throw new AccessCodeProtocolError('Add request is missing the access code payload')
      }
    }

    const stateRecords = await this.fetchAccessCodeStateRecords()
    await this.logAugustAccessCodeRecords('Add preflight August states', stateRecords)

    let records = await this.fetchAccessCodeRecords()
    let context = this.syncAccessCodeContext(records)

    for (const [index, request] of accessCodes.entries()) {
      const mappedRecord = request.identifier
        ? this.findRecordByHomeKitIdentifier(records, context, request.identifier)
        : undefined
      const existingPinRecord = records.find(existing => existing.pin === request.accessCode)
      const existingPinStateRecord = stateRecords.find(existing => existing.pin === request.accessCode)
      let record = mappedRecord ?? existingPinRecord

      if (mappedRecord && mappedRecord.pin !== request.accessCode) {
        if (existingPinStateRecord) {
          if (existingPinStateRecord.state === 'loaded') {
            await this.debugLog(`AccessCode already exists on ${this.accessory.displayName}`)
            record = existingPinRecord ?? existingPinStateRecord
          } else {
            throw new Error(`AccessCode already exists in August ${existingPinStateRecord.state ?? 'unknown'} state`)
          }
        } else {
          const oldPin = mappedRecord.pin
          await this.debugLog(`Modifying AccessCode identifier ${request.identifier?.toString() ?? ''} on ${this.accessory.displayName}`)
          const modifyResult = await this.runAccessCodeApiOperation('modifyPin', async client => await (client as any).modifyPin(this.device.lockId, oldPin, request.accessCode))
          await this.logPinOperationResult('Modify API result', modifyResult)
          records = await this.fetchAccessCodeRecords()
          context = this.syncAccessCodeContext(records)
          record = records.find(existing => existing.pin === request.accessCode)
        }
      } else if (existingPinRecord) {
        await this.debugLog(`AccessCode already exists on ${this.accessory.displayName}`)
        record = existingPinRecord
      } else if (existingPinStateRecord) {
        throw new Error(`AccessCode already exists in August ${existingPinStateRecord.state ?? 'unknown'} state`)
      } else if (!record) {
        const pendingReplacement = request.identifier ? undefined : this.consumePendingAccessCodeDeleteForAdd()
        if (pendingReplacement) {
          await this.debugLog(`Modifying pending-deleted AccessCode identifier ${pendingReplacement.identifier.toString()} on ${this.accessory.displayName}`)

          if (pendingReplacement.record.pin !== request.accessCode) {
            try {
              const modifyResult = await this.runAccessCodeApiOperation('modifyPin', async client => await (client as any).modifyPin(this.device.lockId, pendingReplacement.record.pin, request.accessCode))
              await this.logPinOperationResult('Modify API result', modifyResult)
            } catch (error) {
              this.schedulePendingAccessCodeDelete(pendingReplacement.identifier, pendingReplacement.record)
              throw error
            }
          }

          records = await this.fetchAccessCodeRecords()
          this.restoreAccessCodeMapping(pendingReplacement.identifier, pendingReplacement.record)
          context = this.syncAccessCodeContext(records)
          record = records.find(existing => existing.userId === pendingReplacement.record.userId || existing.pin === request.accessCode)

          if (!record) {
            throw new AccessCodeProtocolError('Access code was not available after modify')
          }

          responseRecords[index] = this.buildAccessCodeRecord(record, context)
          continue
        }

        if (!pendingAddIndexes.has(request.accessCode)) {
          pendingAddIndexes.set(request.accessCode, [])
          pendingAddCodes.push(request.accessCode)
        }
        pendingAddIndexes.get(request.accessCode)!.push(index)
        continue
      }

      if (!record) {
        throw new AccessCodeProtocolError('Access code was not available after add')
      }

      responseRecords[index] = this.buildAccessCodeRecord(record, context)
    }

    if (pendingAddCodes.length > 0) {
      await this.debugLog(`Adding ${pendingAddCodes.length} AccessCode${pendingAddCodes.length === 1 ? '' : 's'} on ${this.accessory.displayName}`)
      const addResults = await this.runAccessCodeApiOperation('addPins', async (client) => {
        const august = client as any
        if (pendingAddCodes.length > 1 && typeof august.addPins === 'function') {
          return await august.addPins(this.device.lockId, pendingAddCodes)
        }

        const addPin = typeof august._addPin === 'function'
          ? august._addPin.bind(august)
          : august.addPin.bind(august)
        const results: any[] = []
        for (const accessCode of pendingAddCodes) {
          results.push(await addPin(this.device.lockId, accessCode))
        }
        return results
      })

      for (const [index, addResult] of addResults.entries()) {
        await this.logPinOperationResult(`Add API result ${index + 1}/${addResults.length}`, addResult)
      }

      await this.logAugustAccessCodeRecords('Add post-write August states', await this.fetchAccessCodeStateRecords())
      records = await this.fetchAccessCodeRecords()
      context = this.syncAccessCodeContext(records)

      for (const accessCode of pendingAddCodes) {
        const record = records.find(existing => existing.pin === accessCode)
        if (!record) {
          throw new AccessCodeProtocolError('Access code was not available after add')
        }

        for (const index of pendingAddIndexes.get(accessCode) ?? []) {
          responseRecords[index] = this.buildAccessCodeRecord(record, context)
        }
      }
    }

    return buildAccessCodeOperationResponse(3, responseRecords.map((record) => {
      if (!record) {
        throw new AccessCodeProtocolError('Access code was not available after add')
      }

      return record
    }))
  }

  private async buildAccessCodeDeleteResponse(identifiers: bigint[]): Promise<string> {
    const responseRecords: ReturnType<typeof this.buildAccessCodeRecord>[] = []

    for (const identifier of identifiers) {
      const records = await this.fetchAccessCodeRecords()
      const context = this.syncAccessCodeContext(records)
      const pendingDelete = this.getPendingAccessCodeDelete(identifier)
      const record = this.findRecordByHomeKitIdentifier(records, context, identifier) ?? pendingDelete?.record

      if (!record) {
        throw new AccessCodeProtocolError(`Access code identifier ${identifier.toString()} was not found for delete`)
      }

      responseRecords.push(this.buildAccessCodeRecord(record, context))
      this.schedulePendingAccessCodeDelete(identifier, record)
      await this.debugLog(`Deferred AccessCode delete identifier ${identifier.toString()} on ${this.accessory.displayName} for ${ACCESS_CODE_DELETE_REPLACE_WINDOW_MS}ms`)
    }

    return buildAccessCodeOperationResponse(5, responseRecords)
  }

  private async runAccessCodeApiOperation<T>(label: string, action: (client: August) => Promise<T>): Promise<T> {
    if (!this.platform.connectivity) {
      throw new Error('accessCode: connectivity not initialized')
    }

    const result = await this.platform.connectivity.execute(
      `accessCode ${label} ${this.device.lockId}`,
      action,
      { throwOnOffline: true },
    )

    if (result === undefined) {
      throw new Error(`August API returned no result for ${label}`)
    }

    return result
  }

  private async setAccessCodeControlPoint(value: CharacteristicValue): Promise<string> {
    let requestType = 'Unknown'

    try {
      const request = parseAccessCodeControlPoint(value)
      requestType = this.accessCodeOperationName(request.operation)
      await this.logHomeKitAccessCodeRequest(requestType, request.records)

      const requestKey = this.accessCodeRequestKey(request)
      const existingPromise = this.accessCodeRequestPromises.get(requestKey)
      if (existingPromise) {
        await this.infoLog(`AccessCode ${requestType} duplicate HomeKit request coalesced while an identical request is already running`)
        return await existingPromise
      }

      const promise = this.enqueueAccessCodeOperation(async () => {
        switch (request.operation) {
          case 1:
            return await this.buildAccessCodeListResponse()
          case 2:
            return await this.buildAccessCodeReadResponse(request.records.map((record) => {
              if (record.identifier === undefined) {
                throw new AccessCodeProtocolError('Read request is missing an access code identifier')
              }
              return record.identifier
            }))
          case 3:
            return await this.buildAccessCodeAddResponse(request.records.map(record => ({
              accessCode: record.accessCode ?? '',
              identifier: record.identifier,
            })))
          case 5:
            return await this.buildAccessCodeDeleteResponse(request.records.map((record) => {
              if (record.identifier === undefined) {
                throw new AccessCodeProtocolError('Delete request is missing an access code identifier')
              }
              return record.identifier
            }))
          default:
            throw new AccessCodeProtocolError(`Unsupported AccessCodeControlPoint operation: ${request.operation}`)
        }
      })

      this.accessCodeRequestPromises.set(requestKey, promise)
      try {
        return await promise
      } finally {
        if (this.accessCodeRequestPromises.get(requestKey) === promise) {
          this.accessCodeRequestPromises.delete(requestKey)
        }
      }
    } catch (error: any) {
      if (error instanceof AccessCodeProtocolError) {
        await this.warnLog(`Invalid AccessCodeControlPoint ${requestType} request on ${this.accessory.displayName}: ${error.message}`)
        return ''
      }

      const status = error?.statusCode ? ` statusCode=${error.statusCode}` : ''
      const step = error?.step ? ` step=${error.step}` : ''
      const rollback = error?.rollback ? ' rollback=attempted' : ''
      await this.errorLog(`AccessCode ${requestType} request failed on ${this.accessory.displayName}:${status}${step}${rollback} ${error?.message ?? error}`)
      throw error
    }
  }

  private firstValue(record: any, keys: string[]): any {
    for (const key of keys) {
      if (record?.[key] !== undefined && record?.[key] !== null) {
        return record[key]
      }
    }

    return undefined
  }

  private firstString(record: any, keys: string[]): string | undefined {
    const value = this.firstValue(record, keys)
    if (value === undefined || value === null) {
      return undefined
    }

    return String(value)
  }

  /**
   * Parse the device status from the August api
   */
  async parseStatus(): Promise<void> {
    await this.debugLog('parseStatus')
    const retryCount = 1
    if (this.lockStatus) {
      if (this.lockStatus.state) {
      // Lock Mechanism
        this.platform.augustConfig?.addSimpleProps(this.lockStatus)
        if (this.LockMechanism && (this.lockStatus.state.unlocking || this.lockStatus.state.locking)) {
          await this.warnLog(`LockCurrentState: ${this.LockMechanism.LockCurrentState}, locking/unlocking parseStatus`
            + ` lockStatus: ${JSON.stringify(this.lockStatus)}`)
        }
        if (!this.device.lock?.hide_lock && this.LockMechanism?.Service && (this.lockStatus.state.locked !== this.lockStatus.state.unlocked)) {
          this.LockMechanism.LockCurrentState = this.lockStatus.state.locked
            ? this.hap.Characteristic.LockCurrentState.SECURED
            : this.lockStatus.state.unlocked
              ? this.hap.Characteristic.LockCurrentState.UNSECURED
              : retryCount > 1 ? this.hap.Characteristic.LockCurrentState.JAMMED : this.hap.Characteristic.LockCurrentState.UNKNOWN
          if (!this.lockUpdateInProgress) {
            this.LockMechanism.LockTargetState = this.LockMechanism.LockCurrentState
          }

          if (this.LockMechanism.LockCurrentState === this.hap.Characteristic.LockCurrentState.UNKNOWN) {
            await this.warnLog(`LockCurrentState: ${this.LockMechanism.LockCurrentState}, (UNKNOWN) parseStatus`
              + ` lockStatus: ${JSON.stringify(this.lockStatus)}`)
          }
          await this.debugLog(`LockCurrentState: ${this.LockMechanism.LockCurrentState}`)
          await this.debugLog(`LockTargetState: ${this.LockMechanism.LockTargetState}`)
        }
        // Contact Sensor
        if (!this.device.lock?.hide_contactsensor && this.ContactSensor?.Service) {
        // ContactSensorState
          this.ContactSensor.ContactSensorState = this.lockStatus.state.open
            ? this.hap.Characteristic.ContactSensorState.CONTACT_NOT_DETECTED
            : this.lockStatus.state.closed
              ? this.hap.Characteristic.ContactSensorState.CONTACT_DETECTED
              : this.lockStatus.doorState?.includes('open')
                ? this.hap.Characteristic.ContactSensorState.CONTACT_NOT_DETECTED
                : this.lockStatus.doorState?.includes('closed')
                  ? this.hap.Characteristic.ContactSensorState.CONTACT_DETECTED
                  : this.ContactSensor.ContactSensorState
          await this.debugLog(`ContactSensorState: ${this.ContactSensor.ContactSensorState}`)
        }
      } else {
        await this.debugWarnLog(`lockStatus state: ${JSON.stringify(this.lockStatus)}`)
      }
    }
    if (this.lockDetails) {
      const lockBatteryLevel = this.lockBatteryLevel(this.lockDetails)
      const keypadBatteryLevel = this.keypadBatteryLevel(this.lockDetails.keypad)
      const keypadLowBattery = keypadBatteryLevel !== undefined
        && this.keypadStatusLowBattery(this.lockDetails.keypad, keypadBatteryLevel) === this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_LOW

      this.Battery.BatteryLevel = keypadBatteryLevel === undefined
        ? lockBatteryLevel
        : Math.min(lockBatteryLevel, keypadBatteryLevel)
      await this.debugLog(`BatteryLevel: ${this.Battery.BatteryLevel} (lock=${lockBatteryLevel},`
        + ` keypad=${keypadBatteryLevel ?? 'unknown'} ${this.lockDetails.keypad?.batteryLevel ?? 'unknown'},`
        + ` raw=${this.lockDetails.keypad?.batteryRaw ?? 'unknown'})`)
      this.Battery.StatusLowBattery = lockBatteryLevel < 15 || keypadLowBattery
        ? this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_LOW
        : this.hap.Characteristic.StatusLowBattery.BATTERY_LEVEL_NORMAL
      await this.debugLog(`StatusLowBattery: ${this.Battery.StatusLowBattery}`)
      // Firmware Version
      if (this.accessory.context.currentFirmwareVersion !== this.lockDetails.currentFirmwareVersion) {
        await this.warnLog(`Firmware Version changed to Current Firmware Version: ${this.lockDetails.currentFirmwareVersion}`)
        this.accessory
          .getService(this.hap.Service.AccessoryInformation)!
          .setCharacteristic(this.hap.Characteristic.HardwareRevision, this.lockDetails.currentFirmwareVersion)
          .setCharacteristic(this.hap.Characteristic.FirmwareRevision, this.lockDetails.currentFirmwareVersion)
          .getCharacteristic(this.hap.Characteristic.FirmwareRevision)
          .updateValue(this.lockDetails.currentFirmwareVersion)
        this.accessory.context.currentFirmwareVersion = this.lockDetails.currentFirmwareVersion
      }
    }
  }

  /**
   * Parse the device status from the August api
   */
  async parseEventStatus(): Promise<void> {
    await this.debugLog('parseEventStatus')
    const retryCount = 1
    if (this.lockEvent) {
      if (this.lockEvent.state) {
        this.debugLog(`lockEvent: ${JSON.stringify(this.lockEvent)}`)
        // Lock Mechanism
        this.platform.augustConfig?.addSimpleProps(this.lockEvent)
        if (this.LockMechanism && (this.lockEvent.state.unlocking || this.lockEvent.state.locking)) {
          await this.debugLog(`is  ${this.lockEvent.state.unlocking ? 'Unlocking' : this.lockEvent.state.locking ? 'Locking' : ''}, parseEventStatus`
            + ` lockEventState: ${JSON.stringify(this.lockEvent.state)}`)
          return
        }
        if (!this.device.lock?.hide_lock && this.LockMechanism?.Service && (this.lockEvent.state.locked !== this.lockEvent.state.unlocked)) {
          this.LockMechanism.LockCurrentState = this.lockEvent.state.locked
            ? this.hap.Characteristic.LockCurrentState.SECURED
            : this.lockEvent.state.unlocked
              ? this.hap.Characteristic.LockCurrentState.UNSECURED
              : retryCount > 1 ? this.hap.Characteristic.LockCurrentState.JAMMED : this.hap.Characteristic.LockCurrentState.UNKNOWN
          if (!this.lockUpdateInProgress) {
            this.LockMechanism.LockTargetState = this.LockMechanism.LockCurrentState
          }

          if (this.LockMechanism.LockCurrentState === this.hap.Characteristic.LockCurrentState.UNKNOWN) {
            await this.warnLog(`LockCurrentState: ${this.LockMechanism.LockCurrentState}, (UNKNOWN) parseEventStatus`
              + ` lockEvent: ${JSON.stringify(this.lockEvent)}`)
          }
          await this.debugLog(`LockCurrentState: ${this.LockMechanism.LockCurrentState}`)
          await this.debugLog(`LockTargetState: ${this.LockMechanism.LockTargetState}`)
        }
        // Contact Sensor
        if (!this.device.lock?.hide_contactsensor && this.ContactSensor?.Service) {
        // ContactSensorState
          this.ContactSensor.ContactSensorState = this.lockEvent.state.open
            ? this.hap.Characteristic.ContactSensorState.CONTACT_NOT_DETECTED
            : this.lockEvent.state.closed
              ? this.hap.Characteristic.ContactSensorState.CONTACT_DETECTED
              : this.lockEvent.doorState === 'open'
                ? this.hap.Characteristic.ContactSensorState.CONTACT_NOT_DETECTED
                : this.lockEvent.doorState === 'closed'
                  ? this.hap.Characteristic.ContactSensorState.CONTACT_DETECTED
                  : this.ContactSensor.ContactSensorState
          await this.debugLog(`ContactSensorState: ${this.ContactSensor.ContactSensorState}`)
        }
      } else {
        await this.debugWarnLog(`lovckEvent state: ${JSON.stringify(this.lockStatus)}`)
      }
    }
  }

  /**
   * Asks the August Home API for the latest device information.
   *
   * Used for the initial fetch in the constructor. Periodic polling is
   * driven by AugustPlatform.startPolling(), which calls applyRefresh()
   * directly with details fetched serially across all registered locks.
   *
   * On failure: returns silently. The ConnectivityManager has already
   * classified the error and (if it's a network error) put itself into
   * 'degraded' state — a probe is scheduled and the next poll cycle
   * will skip until the probe confirms recovery.
   */
  async refreshStatus(): Promise<void> {
    if (this.deviceRefreshRate === 0) {
      await this.debugLog(`(refreshStatus) deviceRefreshRate: ${this.deviceRefreshRate}`)
      return
    }
    if (!this.platform.connectivity) {
      await this.debugLog('(refreshStatus) connectivity not initialized — skipping')
      return
    }
    const lockDetails = await this.platform.connectivity.execute(
      `refreshStatus ${this.accessory.displayName}`,
      client => client.details(this.device.lockId),
    )
    if (lockDetails === undefined) {
      // Either offline, or the call failed. ConnectivityManager has the
      // state machine; nothing to do here.
      return
    }
    await this.applyRefresh(lockDetails as unknown as lockDetails)
  }

  /**
   * Apply a freshly-fetched lockDetails payload to HomeKit characteristics.
   *
   * Public so AugustPlatform.startPolling() can hand-off the result of
   * its serial details() call without going through refreshStatus()
   * (which would re-fetch).
   */
  async applyRefresh(lockDetails: lockDetails): Promise<void> {
    await this.debugSuccessLog(`(applyRefresh) lockDetails: ${JSON.stringify(lockDetails)}`)
    this.lockDetails = lockDetails
    this.lockStatus = lockDetails.LockStatus
    await this.parseStatus()
    await this.updateHomeKitCharacteristics()
  }

  /**
   * Pushes the requested changes to the August API
   */
  async pushChanges(): Promise<void> {
    if (!this.LockMechanism) {
      await this.errorLog(`lockTargetState: ${JSON.stringify(this.LockMechanism)}`)
      return
    }
    const targetState = this.LockMechanism.LockTargetState
    const currentState = this.LockMechanism.LockCurrentState
    if (targetState === currentState) {
      await this.debugLog(`No changes, LockTargetState: ${targetState}, LockCurrentState: ${currentState}`)
      this.LockMechanism.LockTargetState = currentState === this.hap.Characteristic.LockCurrentState.SECURED
        ? this.hap.Characteristic.LockTargetState.SECURED
        : this.hap.Characteristic.LockTargetState.UNSECURED
      await this.updateHomeKitCharacteristics()
      return
    }

    await this.debugLog(`Making API call - Target: ${targetState}, Current: ${currentState}`)

    if (!this.platform.connectivity) {
      await this.errorLog('pushChanges: connectivity not initialized')
      this.resetLockTargetStateToCurrent()
      await this.updateHomeKitCharacteristics()
      return
    }

    try {
      // throwOnOffline:true: user-initiated lock/unlock should fail
      // visibly to HomeKit when the network is down rather than
      // silently dropping the request. The ConnectivityManager
      // classifies the error and updates state, but we re-raise so
      // HomeKit sees a real failure.
      //
      // The manager handles auth/network failures internally
      // (rebuilds the client on auth, schedules a probe on network),
      // so the explicit retry block that used to live here is gone.
      // If the user retries the action, the manager will either be
      // recovered by then or still offline (and throw OfflineError
      // again).
      await this.platform.connectivity.execute(
        `pushChanges ${this.device.lockId}`,
        async (client) => {
          if (targetState === this.hap.Characteristic.LockTargetState.UNSECURED) {
            await client.unlock(this.device.lockId)
          } else {
            await client.lock(this.device.lockId)
          }
        },
        { throwOnOffline: true },
      )
      await this.successLog(`Sending request to August API: ${targetState === 1 ? 'Locked' : 'Unlocked'}`)
      await this.updateHomeKitCharacteristics()
    } catch (e: any) {
      await this.statusCode('pushChanges', e)
      await this.errorLog(`pushChanges: ${e.message ?? e}`)
      this.resetLockTargetStateToCurrent()
      await this.updateHomeKitCharacteristics()
    }
  }

  private resetLockTargetStateToCurrent(): void {
    if (!this.LockMechanism) {
      return
    }

    this.LockMechanism.LockTargetState = this.LockMechanism.LockCurrentState === this.hap.Characteristic.LockCurrentState.SECURED
      ? this.hap.Characteristic.LockTargetState.SECURED
      : this.hap.Characteristic.LockTargetState.UNSECURED
  }

  /**
   * Updates the status for each of the HomeKit Characteristics
   */
  async updateHomeKitCharacteristics(): Promise<void> {
    await this.debugLog('updateHomeKitCharacteristics')
    // Lock Mechanism
    if (!this.device.lock?.hide_lock && this.LockMechanism?.Service) {
      // LockTargetState
      await this.updateCharacteristic(this.LockMechanism.Service, 'LockMechanism', this.hap.Characteristic.LockTargetState, this.LockMechanism.LockTargetState, 'LockTargetState')
      // LockCurrentState
      await this.updateCharacteristic(this.LockMechanism.Service, 'LockMechanism', this.hap.Characteristic.LockCurrentState, this.LockMechanism.LockCurrentState, 'LockCurrentState', 1, 'Locked', 'Unlocked')
    }
    // Lock Battery
    await this.updateCharacteristic(this.Battery.Service, 'Battery', this.hap.Characteristic.BatteryLevel, this.Battery.BatteryLevel, 'BatteryLevel')
    await this.updateCharacteristic(this.Battery.Service, 'Battery', this.hap.Characteristic.StatusLowBattery, this.Battery.StatusLowBattery, 'StatusLowBattery')
    // Contact Sensor
    if (!this.device.lock?.hide_contactsensor && this.ContactSensor?.Service) {
      // ContactSensorState
      await this.updateCharacteristic(this.ContactSensor.Service, 'ContactSensor', this.hap.Characteristic.ContactSensorState, this.ContactSensor.ContactSensorState, 'ContactSensorState', 1, 'Opened', 'Closed')
    }
  }

  async setLockTargetState(value: CharacteristicValue): Promise<void> {
    if (this.LockMechanism) {
      if (this.LockMechanism.LockTargetState !== this.LockMechanism.LockCurrentState) {
        await this.debugLog(`Set LockTargetState: ${value}`)
      } else {
        await this.debugLog(`No changes, LockTargetState: ${this.LockMechanism.LockTargetState},`
          + ` LockCurrentState: ${this.LockMechanism.LockCurrentState}`)
      }

      this.accessory.context.LockMechanismLockTargetState = this.LockMechanism.LockTargetState = value
      this.doLockUpdate.next()
    }
  }

  // Holds the PubNub status-listener unsubscribe alongside the channel
  // unsubscribe (this.pubnubUnsubscribe). Both are owned by the same
  // August instance underneath; both need to be torn down on lock
  // removal.
  private pubnubStatusUnsubscribe?: () => void
  // The August instance that owns the PubNub WebSocket for THIS lock.
  // Kept so we can register status listeners on it. Distinct from
  // platform.connectivity's August (which is for HTTP only).
  private pubnubAugust?: August

  async subscribeAugust(): Promise<void> {
    await this.debugLog('subscribeAugust')
    await this.platform.augustCredentials()
    if (this.config.credentials) {
      // Clean up any previous subscription before creating a new one.
      // This is defensive: subscribeAugust() is currently only called once
      // from the constructor, but this makes the method idempotent and safe
      // to call again in the future.
      this.tearDownPubNubSubscription()

      const normalizedCredentials = await this.platform.getNormalizedCredentials()

      // Construct an August instance manually so we can register a
      // PubNub status listener on the same instance that owns the
      // channel subscription. The previous code used the static
      // August.subscribe(), which hides the instance and made it
      // impossible to register status listeners.
      this.pubnubAugust = new August(normalizedCredentials)

      // Status listener: feed PubNub reconnect events into the
      // platform's ConnectivityManager. PubNub's WebSocket reconnects
      // seconds before HTTP polling would notice the network is back,
      // so this is the fastest signal connectivity has recovered.
      // Defensive guard around connectivity in case this method is
      // ever called before the manager is initialized.
      this.pubnubStatusUnsubscribe = this.pubnubAugust.onPubNubStatus((status: any) => {
        const category = status?.category
        if (category === 'PNConnectedCategory' || category === 'PNReconnectedCategory') {
          this.debugLog(`PubNub status: ${category} — signalling connectivity recovery`)
          this.platform.connectivity?.onPubNubReconnect()
        } else if (category === 'PNNetworkDownCategory' || category === 'PNDisconnectedCategory') {
          this.debugLog(`PubNub status: ${category}`)
        }
      })

      this.pubnubUnsubscribe = await this.pubnubAugust.subscribe(this.device.lockId, async (AugustEvent: lockEvent, timestamp: Date) => {
        await this.debugLog(`AugustEvent: ${JSON.stringify(AugustEvent)}, ${JSON.stringify(timestamp)}`)
        // Update HomeKit
        this.lockEvent = AugustEvent
        await this.parseEventStatus()
        await this.updateHomeKitCharacteristics()
      })
    } else {
      await this.errorLog('subscribeAugust: No credentials')
    }
  }

  /**
   * Tear down the PubNub subscription for this lock. Safe to call multiple
   * times and when no subscription exists. Called on lock removal to free
   * the PubNub instance, WebSocket connection, and listener.
   */
  tearDownPubNubSubscription(): void {
    for (const pendingDelete of this.pendingAccessCodeDeletes.values()) {
      clearTimeout(pendingDelete.timer)
    }
    this.pendingAccessCodeDeletes.clear()

    if (this.pubnubStatusUnsubscribe) {
      try {
        this.pubnubStatusUnsubscribe()
      } catch (e: any) {
        this.debugLog(`Error tearing down PubNub status listener: ${e.message || e}`)
      }
      this.pubnubStatusUnsubscribe = undefined
    }
    if (this.pubnubUnsubscribe) {
      try {
        this.pubnubUnsubscribe()
      } catch (e: any) {
        this.debugLog(`Error tearing down PubNub subscription: ${e.message || e}`)
      }
      this.pubnubUnsubscribe = undefined
    }
    if (this.pubnubAugust) {
      try {
        this.pubnubAugust.destroy()
      } catch (e: any) {
        this.debugLog(`Error destroying PubNub August instance: ${e.message || e}`)
      }
      this.pubnubAugust = undefined
    }
  }
}
