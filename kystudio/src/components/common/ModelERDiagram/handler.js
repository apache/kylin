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
      v.joins.forEach(item => {
        that.primaryKeys.push(item.primaryKey)
        that.foreignKeys.push(item.foreignKey)
        addPlumbPoints(plumbTool, v.guid)
        addPlumbPoints(plumbTool, item.guid)
        const conn = plumbTool.connect(v.guid, item.guid, () => {}, {
          joinType: v.type ?? '',
          brokenLine: false
        })
        allConnectList[`${v.guid}$${item.guid}`] = conn
      })
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
