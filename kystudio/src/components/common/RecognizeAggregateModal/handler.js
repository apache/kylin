import { acequire } from 'brace'
import Vue from 'vue'

const { Range } = acequire('ace/range')

const dom = acequire('ace/lib/dom')

export const ERROR_TYPE = {
  COLUMN_NOT_IN_MODEL: 'columnNotInModel',
  COLUMN_NOT_IN_INCLUDES: 'columnNotInIncludes',
  COLUMN_DUPLICATE: 'columnDuplicate'
}

export function $updatePlaceholder (editor, renderPlaceholder) {
  const value = editor.renderer.$composition || editor.getValue()
  if (value && editor.renderer.placeholderNode) {
    editor.renderer.off('afterRender', editor.$updatePlaceholder)
    dom.removeCssClass(editor.container, 'ace_hasPlaceholder')
    editor.renderer.placeholderNode.remove()
    editor.renderer.placeholderNode = null
  } else if (!value && !editor.renderer.placeholderNode) {
    editor.renderer.on('afterRender', editor.$updatePlaceholder)
    dom.addCssClass(editor.container, 'ace_hasPlaceholder')
    var el = dom.createElement('div')
    editor.renderer.placeholderNode = el
    editor.renderer.content.appendChild(editor.renderer.placeholderNode)

    var vmEl = dom.createElement('div')
    el.appendChild(vmEl)
    editor.renderer.placeholderVm = new Vue({ el: vmEl, render: renderPlaceholder })
  }
}

export function updatePlaceHolder (editor, renderPlaceholder) {
  if (editor) {
    if (!editor.$updatePlaceholder) {
      editor.$updatePlaceholder = $updatePlaceholder.bind(this, editor, renderPlaceholder)
      editor.on('input', editor.$updatePlaceholder)
    }
    editor.$updatePlaceholder(editor, renderPlaceholder)
  }
}

export function refreshEditor (editor) {
  if (editor) {
    editor.resize(true)
  }
}

export function clearupMarkers (editor) {
  const session = editor.getSession()
  for (const marker of Object.values(session.getMarkers())) {
    if (marker.type === 'fullLine') {
      session.removeMarker(marker.id)
    }
  }
}

export function scrollToLineAndHighlight (editor, line) {
  const session = editor.getSession()
  if (line !== undefined) {
    clearupMarkers(editor)
    editor.scrollToLine(line, true)
    const range = new Range(line, 0, line, 1)
    session.addMarker(range, 'ace_active-line', 'fullLine')
  }
}

export function searchColumnInEditor (editor, column) {
  const { $search: editorSearch } = editor
  const session = editor.getSession()

  editorSearch.setOptions({
    needle: `^${column},\n`,
    caseSensitive: true,
    wholeWord: false,
    regExp: true
  })
  return editorSearch.findAll(session)
}

export function collectErrorsInEditor (errors, editor) {
  const { notInModel, duplicate, notInIncludes } = errors

  let errorInEditor = []
  let errorLines = []

  for (const column of notInModel) {
    const notInModelRanges = searchColumnInEditor(editor, column)
    errorInEditor = [...errorInEditor, ...notInModelRanges.map(r => {
      errorLines.push(r.start.row)
      return { row: r.start.row, column, type: ERROR_TYPE.COLUMN_NOT_IN_MODEL }
    })]
  }

  for (const column of notInIncludes) {
    const notInIncludesRanges = searchColumnInEditor(editor, column)
    errorInEditor = [...errorInEditor, ...notInIncludesRanges.map(r => {
      errorLines.push(r.start.row)
      return { row: r.start.row, column, type: ERROR_TYPE.COLUMN_NOT_IN_INCLUDES }
    })]
  }

  for (const column of duplicate) {
    const [, ...duplicateRanges] = searchColumnInEditor(editor, column)
    errorInEditor = [...errorInEditor, ...duplicateRanges.map(r => {
      errorLines.push(r.start.row)
      return { row: r.start.row, column, type: ERROR_TYPE.COLUMN_DUPLICATE }
    })]
  }

  errorLines = errorLines.sort()

  return { errorInEditor, errorLines }
}
