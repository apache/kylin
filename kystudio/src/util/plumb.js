import { jsPlumb } from 'jsplumb'
import { stopPropagation } from './event'
import { createToolTipDom } from './domHelper'
// jsPlumb 工具库
export function jsPlumbTool () {
  var plumbInstance = null
  let lineColor = '#3c8dbc'
  let hoverLineColor = '#9DCEFB'
  let brokenColor = '#E03B3B'
  let hoverBrokenColor = '#CA1616'
  let strokeWidth = 1
  return {
    endpointConfig: {
      endpoint: [
        'Dot', {
          cssClass: 'line-end-dot'
        }
      ],
      paintStyle: {
        // stroke: lineColor,
        fill: hoverLineColor,
        radius: 2,
        strokeWidth: 1
      },
      hoverPaintStyle: {
        stroke: hoverLineColor,
        fill: hoverLineColor
      },
      isSource: true,
      isTarget: true,
      connector: [ 'Flowchart', { stub: 1, gap: 1, cornerRadius: 3, alwaysRespectStubs: true } ], // 设置连线为直角曲线
      // connector: [ 'Bezier', { curviness: 22 } ],
      connectorStyle: {
        strokeWidth: strokeWidth,
        stroke: lineColor
      },
      connectorHoverStyle: {
        stroke: hoverLineColor,
        strokeWidth: 2
      },
      dragOptions: {}
    },
    columnEndPointConfig: {
      endpoint: [
        'Blank', {
          cssClass: 'column-point-dot'
        }
      ],
      // paintStyle: {
      //   radius: 5
      //   // fill: '#0875DA'
      // },
      connectorStyle: {
        strokeWidth: 2,
        stroke: hoverLineColor,
        joinstyle: 'round'
      },
      endpointStyle: {
        zIndex: 25,
        outlineWidth: 3,
        fill: '#fff',
        radius: 4
      },
      // endpointHoverStyle: {
      //   radius: 6
      //   // outlineWidth: 3,
      //   // outlineColor: '#0875DA',
      //   // outlineStroke: '#0875DA'
      // },
      isSource: true,
      isTarget: true,
      connector: [ 'Flowchart', { stub: 1, gap: 1, cornerRadius: 3, alwaysRespectStubs: true } ], // 设置连线为直角曲线
      maxConnections: 1,
      dropOptions: {
        hoverClass: 'hoverDrop'
      }
    },
    init: function (dom, zoom) {
      plumbInstance = this._getPlumbInstance(jsPlumb, dom)
      this.setZoom(zoom)
      return plumbInstance
    },
    setLineStyle (type) {
      if (type !== 'broken') {
        this.endpointConfig.paintStyle.stroke = lineColor
        this.endpointConfig.hoverPaintStyle.fill = hoverLineColor
        this.endpointConfig.connectorStyle.stroke = lineColor
        this.endpointConfig.connectorHoverStyle.stroke = hoverLineColor
        return
      }
      this.endpointConfig.paintStyle.stroke = brokenColor
      this.endpointConfig.hoverPaintStyle.fill = hoverBrokenColor
      this.endpointConfig.connectorStyle.stroke = brokenColor
      this.endpointConfig.connectorHoverStyle.stroke = hoverBrokenColor
      this.endpointConfig.connectorStyle['cssClass'] = 'broken-connector'
    },
    lazyRender (cb) {
      jsPlumb.setSuspendDrawing(true)
      cb && cb()
      jsPlumb.setSuspendDrawing(false, true)
    },
    bindConnectionEvent (cb) {
      plumbInstance.bind('connection', (info, originalEvent) => {
        cb(info.connection, originalEvent)
      })
      plumbInstance.bind('connectionAborted', (info, originalEvent) => {
        cb(info, originalEvent, 'connectionAborted')
      })
      plumbInstance.bind('beforeDrag', (info, originalEvent) => {
        cb(info, null, 'dragPoint')
      })
      plumbInstance.bind('beforeDrop', (info, originalEvent) => {
        cb(info, null, 'dropPoint')
      })
      plumbInstance.bind('click', (info, originalEvent) => {
        cb(info, null, 'lineClick')
      })
      plumbInstance.bind('connectionDrag', (info, originalEvent) => {
        cb(info, originalEvent, 'connectionDrag')
      })
    },
    deleteAllEndPoints () {
      plumbInstance.deleteEveryEndpoint()
    },
    deleteEndPoint (uuid) {
      plumbInstance.deleteEndpoint(uuid)
    },
    removeAllEndpoints (el) {
      plumbInstance.removeAllEndpoints(el)
    },
    addEndpoint (guid, anchor, endPointConfig) {
      return plumbInstance.addEndpoint(guid, anchor, endPointConfig)
    },
    _getPlumbInstance (jsPlumb, el) {
      return jsPlumb.getInstance({
        DragOptions: { cursor: 'pointer', zIndex: 2000 },
        HoverPaintStyle: this.endpointConfig.connectorStyle,
        ConnectionOverlays: [
          // [ 'Arrow', {
          //   location: 1,
          //   visible: true,
          //   width: 11,
          //   length: 11,
          //   id: 'ARROW',
          //   foldback: 2
          // } ],
          // [ 'Custom', {
          //   location: 1,
          //   create: (component) => {
          //     const dom = document.createElement('span')
          //     dom.innerHTML = '<span class="line-end-pointer">&bull;</span>'
          //     return dom
          //   },
          //   id: 'customPoint',
          //   cssClass: 'line-end'
          // }]
        ],
        Container: el
      })
    },
    refreshPlumbInstance () {
      plumbInstance.repaintEverything()
    },
    draggable (idList, stopCb) {
      plumbInstance.draggable(idList, {
        handle: '.table-title',
        drop: function (e) {
        },
        stop: function (e) {
          stopCb(e)
        }
      })
    },
    setZoom (zoom) {
      var transformOrigin = [0.5, 0.5]
      var el = plumbInstance.getContainer()
      var p = [ 'webkit', 'moz', 'ms', 'o' ]
      var s = 'scale(' + zoom + ') translateZ(0)'
      var oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%'
      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s
        el.style[p[i] + 'TransformOrigin'] = oString
      }
      el.style['transform'] = s
      el.style['transformOrigin'] = oString
      plumbInstance.setZoom(zoom)
    },
    connect (pid, fid, clickCb, otherProper) {
      const child = document.createElement('i')
      child.className = 'close-icon el-ksd-n-icon-close-outlined'
      const hideNode = document.createElement('span')
      hideNode.className = 'join-type-hide'

      var defaultPata = {
        uuids: [fid, pid],
        deleteEndpointsOnDetach: true,
        editable: true,
        cssClass: `${fid}&${pid}`,
        overlays: [
          ['Custom', {
            create: function () {
              let overlays = !otherProper.brokenLine ? createToolTipDom(`<span class="join-type ${otherProper.joinType === 'INNER' ? 'el-ksd-n-icon-inner-join-filled' : otherProper.joinType === 'LEFT' ? 'el-ksd-n-icon-left-join-filled' : 'el-ksd-n-icon-right-join-filled'}"></span>`, {
                text: otherProper.joinType,
                children: [hideNode, child]
              }) : createToolTipDom(`<span class="join-type el-ksd-icon-wrong_fill_16"></span><i class='close-icon el-ksd-n-icon-close-outlined'></i>`, {
                text: otherProper.joinType,
                children: [hideNode, child]
              })
              return overlays
            },
            id: pid + (fid + 'label'),
            events: {
              mousedown: function (_, e) {
                stopPropagation(e)
                return false
              },
              click: function (_, e) {
                clickCb(pid, fid, e)
              }
            }
          }]
        ]
      }
      defaultPata = Object.assign(defaultPata, otherProper)
      return plumbInstance.connect(defaultPata)
    },
    deleteConnect (conn) {
      if (conn.endpoints) {
        conn.endpoints.forEach((point) => {
          this.deleteEndPoint(point) // 删除连接点，等同删除连线
        })
      }
    }
  }
}
