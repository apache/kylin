/*jshint undef: false, unused: false, indent: 2*/
/*global angular: false */

'use strict';

angular.module('demoApp').service('BoardService', ['$modal', 'BoardManipulator', function ($modal, BoardManipulator) {

  return {
    removeCard: function (board, column, card) {
      if (confirm('Are You sure to Delete?')) {
        BoardManipulator.removeCardFromColumn(board, column, card);
      }
    },

    addNewCard: function (board, column) {
      var modalInstance = $modal.open({
        templateUrl: 'views/partials/newCard.html',
        controller: 'NewCardController',
        backdrop: 'static',
        resolve: {
          column: function () {
            return column;
          }
        }
      });
      modalInstance.result.then(function (cardDetails) {
        if (cardDetails) {
          BoardManipulator.addCardToColumn(board, cardDetails.column, cardDetails.title, cardDetails.details);
        }
      });
    },
    kanbanBoard: function (board) {
      var kanbanBoard = new Board(board.name, board.numberOfColumns);
      angular.forEach(board.columns, function (column) {
        BoardManipulator.addColumn(kanbanBoard, column.name);
        angular.forEach(column.cards, function (card) {
          BoardManipulator.addCardToColumn(kanbanBoard, column, card.title, card.details);
        });
      });
      return kanbanBoard;
    },
    sprintBoard: function (board) {
      var sprintBoard = new Board(board.name, board.numberOfColumns);
      angular.forEach(board.columns, function (column) {
        BoardManipulator.addColumn(sprintBoard, column.name);
      });
      angular.forEach(board.backlogs, function (backlog) {
        BoardManipulator.addBacklog(sprintBoard, backlog.title);
        angular.forEach(backlog.phases, function (phase) {
          BoardManipulator.addPhaseToBacklog(sprintBoard, backlog.title, phase);
          angular.forEach(phase.cards, function (card) {
            BoardManipulator.addCardToBacklog(sprintBoard, backlog.title, phase.name, card);
          });
        });

      });
      return sprintBoard;
    }
  };
}]);