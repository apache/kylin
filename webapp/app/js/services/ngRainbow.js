var module = angular.module('ngRainbow', []);
module.provider('rainbowBar', function () {
    "use strict";
    var defaultOpts = {
        autoRun: true,
        barThickness: 3,
        barColors: {
            '0': 'rgba(26,  188, 156, .9)',
            '.25': 'rgba(52,  152, 219, .9)',
            '.50': 'rgba(241, 196, 15,  .9)',
            '.75': 'rgba(230, 126, 34,  .9)',
            '1.0': 'rgba(211, 84,  0,   .9)'
        },
        shadowBlur: 10,
        shadowColor: 'rgba(0,   0,   0,   .6)'
    };

    this.configure = function (opts) {
        Object.keys(opts).forEach(function (key) {
            if (defaultOpts.hasOwnProperty(key)) {
                defaultOpts[key] = opts[key]
            }
        })
    };

    this.$get = ['$document', '$window', function ($document, $window) {
        (function () {
            var lastTime = 0;
            var vendors = ['ms', 'moz', 'webkit', 'o'];
            for (var x = 0; x < vendors.length && !$window.requestAnimationFrame; ++x) {
                $window.requestAnimationFrame = $window[vendors[x] + 'RequestAnimationFrame'];
                $window.cancelAnimationFrame = $window[vendors[x] + 'CancelAnimationFrame']
                    || $window[vendors[x] + 'CancelRequestAnimationFrame'];
            }
            if (!$window.requestAnimationFrame)
                $window.requestAnimationFrame = function (callback) {
                    var currTime = new Date().getTime();
                    var timeToCall = Math.max(0, 16 - (currTime - lastTime));
                    var id = $window.setTimeout(function () {
                            callback(currTime + timeToCall);
                        },
                        timeToCall);
                    lastTime = currTime + timeToCall;
                    return id;
                };
            if (!$window.cancelAnimationFrame)
                $window.cancelAnimationFrame = function (id) {
                    clearTimeout(id);
                };
        }());

        var canvas, progressTimerId, fadeTimerId, currentProgress, showing,
            addEvent = function (elem, type, handler) {
                if (elem.addEventListener) {
                    elem.addEventListener(type, handler, false);
                } else if (elem.attachEvent) {
                    elem.attachEvent('on' + type, handler);
                } else {
                    elem['on' + type] = handler;
                }
            },
            options = defaultOpts,
            repaint = function () {
                canvas.width = $window.innerWidth;
                canvas.height = options.barThickness * 5; // need space for shadow

                var ctx = canvas.getContext('2d');
                ctx.shadowBlur = options.shadowBlur;
                ctx.shadowColor = options.shadowColor;

                var lineGradient = ctx.createLinearGradient(0, 0, canvas.width, 0);
                Object.keys(options.barColors).forEach(function(stop) {
                    lineGradient.addColorStop(stop, options.barColors[stop])
                });
                ctx.lineWidth = options.barThickness;
                ctx.beginPath();
                ctx.moveTo(0, options.barThickness / 2);
                ctx.lineTo(Math.ceil(currentProgress * canvas.width), options.barThickness / 2);
                ctx.strokeStyle = lineGradient;
                ctx.stroke();
            },
            createCanvas = function () {
                canvas = document.createElement('canvas');
                var style = canvas.style;
                style.position = 'fixed';
                style.top = style.left = style.right = style.margin = style.padding = 0;
                style.zIndex = 100001;
                style.display = 'none';
                document.body.appendChild(canvas);
                addEvent($window, 'resize', repaint);
            },
            progress = function(to) {
                if (typeof to === "undefined")
                    return currentProgress;
                if (typeof to === "string")
                    to = (to.indexOf('+') !== -1 || to.indexOf('-') !== -1)
                        ? currentProgress + eval('0' + to)
                        : parseFloat(to);
                currentProgress = to > 1 ? 1 : to;
                repaint();
                return currentProgress;
            };
        return {
            show: function () {
                if (showing) return;
                showing = true;
                if (fadeTimerId !== null)
                    $window.cancelAnimationFrame(fadeTimerId);
                if (!canvas) createCanvas();
                canvas.style.opacity = 1;
                canvas.style.display = 'block';
                progress(0);
                if (options.autoRun) {
                    (function loop() {
                        progressTimerId = $window.requestAnimationFrame(loop);
                        progress('+' + (.05 * Math.pow(1 - Math.sqrt(currentProgress), 2)));
                    })()
                }
            },
            hide: function () {
                if (!showing) {
                    return;
                }
                showing = false;
                if (progressTimerId != null) {
                    $window.cancelAnimationFrame(progressTimerId);
                    progressTimerId = null;
                }
                (function loop() {
                    if (progress('+.1') >= 1) {
                        canvas.style.opacity -= .05;
                        if (canvas.style.opacity <= .05) {
                            canvas.style.display = 'none';
                            fadeTimerId = null;
                            return;
                        }
                    }
                    fadeTimerId = $window.requestAnimationFrame(loop);
                })();
            },
            addProgress: function(percent) {
                progress(percent);
            }
        }

    }]
});