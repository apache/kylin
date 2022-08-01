<script>
import { formatConflictsGroupByName, conflictTypes } from './handler'

function formatRowString (conflictType, conflictItem) {
  switch (conflictType) {
    case conflictTypes.COLUMN_NOT_EXISTED: {
      const tableFullName = conflictItem.element.split('-')[1]
      const columnName = conflictItem.content
      return `${tableFullName}.${columnName}`
    }
    case conflictTypes.INVALID_COLUMN_DATATYPE: {
      const [, tableFullName, columnName] = conflictItem.element.split('-')
      return `${tableFullName}.${columnName}`
    }
    case conflictTypes.TABLE_CHANGED:
    case conflictTypes.TABLE_NOT_EXISTED:
    default:
      return conflictItem.content
  }
}

function renderConflictBox (h, conflictType, conflictRows = []) {
  const messages = conflictRows.map(([conflictItem]) => formatRowString(conflictType, conflictItem))
  const autoSizeProps = { minRows: 1, maxRows: 6 }
  return <el-input class="conflict-box" autosize={autoSizeProps} readonly type="textarea" value={messages.join('\r\n')} />
}

export default {
  functional: true,
  render (h, context) {
    const { props, parent } = context
    const conflictGroups = formatConflictsGroupByName(props.conflicts)
    const modelDuplicate = conflictGroups.filter(group => group.type === conflictTypes.DUPLICATE_MODEL_NAME)
    const modelConflicts = conflictGroups.filter(group => group.type !== conflictTypes.DUPLICATE_MODEL_NAME)

    return (
      <div class="model-conflicts">
        {modelDuplicate.length && !modelConflicts.length ? (
          <div class="message">{parent.$t(conflictTypes.DUPLICATE_MODEL_NAME)}</div>
        ) : null}
        {!modelDuplicate.length && modelConflicts.length ? (
          <div class="message">{parent.$t('METADATA_CONFLICT')}</div>
        ) : null}
        {modelDuplicate.length && modelConflicts.length ? (
          <div class="message">{parent.$t('DUPLICATE_MODEL_NAME_AND_METADATA_CONFLICT')}</div>
        ) : null}
        {modelConflicts.map(group => (
          <div class="conflict-item" key={group.type}>
            <div class="conflict-title">{parent.$t(group.type)} ({group.conflicts.length})</div>
            {renderConflictBox(h, group.type, group.conflicts)}
          </div>
        ))}
      </div>
    )
  }
}
</script>
