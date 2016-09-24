(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .directive('fileread', fileread);

  /** @ngInject */
  function fileread($window, $log) {

    var directive = {
        scope: {
            fileread: "="
        },
        link: function (scope, element, attributes) {
            element.bind("change", function (changeEvent) {
                /*var reader = new FileReader();
                reader.onload = function (loadEvent) {
                    scope.$apply(function () {
                        scope.fileread = loadEvent.target.result;
                    });
                }
                reader.readAsDataURL(changeEvent.target.files[0]);*/
                scope.$apply(function () {
                    scope.fileread = changeEvent.target.files[0];
                });
            });
        }
    };

    return directive;
  }

})();

