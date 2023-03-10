import { jsPlumbTool } from '../../../util/plumb'
import { modelRenderConfig } from '../../studio/StudioModel/ModelEdit/config'
import ModelTree from '../../studio/StudioModel/ModelEdit/layout'

let allConnectList = {}

export function initPlumb (renderDom, zoom) {
  allConnectList = {}
  const plumbs = jsPlumbTool()
  return {
    plumbInstance: plumbs.init(renderDom, zoom / 10),
    plumbTool: plumbs
  }
}

// 表连线
export function drawLines (that, plumbTool, joints) {
  plumbTool.lazyRender(() => {
    joints.forEach(v => {
      const [firstJoin] =  v.joins.slice(0, 1)
      const primaryKey = []
      const foreignKey = []
      v.joins.forEach(item => {
        primaryKey.push(item.primaryKey.split('.')[1])
        foreignKey.push(item.foreignKey.split('.')[1])
        that.primaryKeys.push(item.primaryKey)
        that.foreignKeys.push(item.foreignKey)
      })
      addPlumbPoints(plumbTool, v.guid)
      addPlumbPoints(plumbTool, firstJoin.guid)
      const conn = plumbTool.connect(v.guid, firstJoin.guid, () => {}, {
        joinType: v.type ?? '',
        brokenLine: false
      })
      const labelLayout = conn.getOverlay(v.guid + (firstJoin.guid + 'label'))
      const labelCanvas = labelLayout.canvas
      const lineCanvas = conn.canvas

      allConnectList[`${v.guid}$${firstJoin.guid}`] = conn
      handleHoverLinks(labelCanvas, {fKeys: foreignKey, pKeys: primaryKey, fid: firstJoin.guid, pid: v.guid})
      createAndUpdateSvgGroup(lineCanvas, {type: 'create', isBroken: false, fKeys: foreignKey, pKeys: primaryKey, fid: firstJoin.guid, pid: v.guid})
    })
  })
}

// 添加端点
function addPlumbPoints (plumbTool, guid) {
  const anchor = modelRenderConfig.jsPlumbAnchor
  const scope = 'showlink'
  const endPointConfig = Object.assign({}, plumbTool.endpointConfig, {
    scope: scope,
    uuid: guid
  })

  plumbTool.addEndpoint(guid, {anchor: anchor}, endPointConfig)
}

// 根据树形结构自定义表的位置
export function customCanvasPosition (vm, renderDom, model, zoom) {
  const { tables, canvas: currentCanvas } = model
  const canvas = {
    coordinate: {},
    zoom: zoom
  }
  const layersRoot = autoCalcLayer(tables, model)
  let layers = layersRoot?.rightNodes?.db
  if (layersRoot?.leftNodes) {
    layers = [...layers, ...layersRoot.leftNodes.db]
  }
  if (layers && layers.length > 0) {
    const baseL = modelRenderConfig.baseLeft
    const baseT = modelRenderConfig.baseTop
    const renderDomBound = renderDom.getBoundingClientRect()
    const centerL = renderDomBound.width / 2 - modelRenderConfig.tableBoxWidth / 2
    const moveL = layers[0].X - centerL
    for (let k = 0; k < layers.length; k++) {
      const currentT = layers[k].tree.tables[layers[k].guid]
      const centerT = renderDomBound.height / 3 - (currentT?.drawSize?.height ?? modelRenderConfig.tableBoxHeight) / 2
      const moveT = layers[0].Y - centerT
      let [currentTable] = tables.filter(item => item.guid === layers[k].guid)
      canvas.coordinate[`${currentTable.alias}`] = {
        x: baseL - moveL + layers[k].X,
        y: baseT - moveT + layers[k].Y,
        width: modelRenderConfig.tableBoxWidth,
        height: currentCanvas?.coordinate?.[`${currentTable.alias}`]?.height ?? modelRenderConfig.tableBoxHeight
      }
    }
  }
  return canvas
}

// 获取树形结构
function autoCalcLayer (tables, model) {
  const { canvas: currentCanvas } = model
  const [factTable] = tables.filter(it => it.type === 'FACT')
  if (!factTable) {
    return
  }
  const tbs = {}
  tables.forEach(it => {
    tbs[it.guid] = {
      ...it,
      drawSize: {...currentCanvas.coordinate[`${it.alias}`]}
    }
  })
  const rootGuid = factTable.guid
  const tree = new ModelTree({rootGuid: rootGuid, showLinkCons: allConnectList, tables: tbs})
  tree.positionTree()
  return tree.nodeDB.rootNode
}

// 自定义扩大 line svg hover 热区
export function createAndUpdateSvgGroup (lineCanvas, conn, guid) {
  if (conn.type === 'create') {
    const sign = 'http://www.w3.org/2000/svg'
    const path = lineCanvas.firstChild
    if (!path) return
    if (lineCanvas.querySelector('g')) return
    const newPath = path.cloneNode(true)
    const group = document.createElementNS(sign, 'g')
    const d = path.getAttribute('d')

    group.id = conn.isBroken ? 'broken-use-group' : 'use-group'
    newPath.setAttribute('d', d)
    newPath.setAttribute('stroke-width', 10)
    newPath.setAttribute('stroke', 'transparent')
    newPath.setAttribute('id', 'use')
    group.appendChild(path)
    group.appendChild(newPath)
    lineCanvas.appendChild(group)

    handleHoverLinks(group, conn)
  } else {
    const lineGroups = !guid ? Object.keys(allConnectList): Object.keys(allConnectList).filter(it => it.split('$').includes(guid))
    lineGroups.forEach(item => {
      const line = allConnectList[item].canvas
      if (line.querySelector('g')) {
        const paths = line.querySelectorAll('path')
        const firstPathLine = paths[0].getAttribute('d')
        paths[1].setAttribute('d', firstPathLine)
      } else {
        // createAndUpdateSvgGroup(line, allConnectList[item].isBroken, 'create')
      }
    })
  }
}

function handleHoverLinks (element, conn) {
  element.onmouseenter = function () {
    document.getElementById(`${conn.fid}`).className += ' link-hover'
    document.getElementById(`${conn.pid}`).className += ' link-hover'
    conn.fKeys.forEach(item => document.getElementById(`${conn.fid}_${item}`).className += ' is-hover')
    conn.pKeys.forEach(item => document.getElementById(`${conn.pid}_${item}`).className += ' is-hover')
  }
  element.onmouseleave = function () {
    document.getElementById(`${conn.fid}`).classList.remove('link-hover')
    document.getElementById(`${conn.pid}`).classList.remove('link-hover')
    conn.fKeys.forEach(item => document.getElementById(`${conn.fid}_${item}`).classList.remove('is-hover'))
    conn.pKeys.forEach(item => document.getElementById(`${conn.pid}_${item}`).classList.remove('is-hover'))
  }
}
