require('./view.css');

module.exports = {
  controller: ViewController,
  controllerAs: 'view',
  template: require('./view.html')
};

/** @ngInject */
function ViewController($timeout, $log, $location, $filter, $uibModal, preparationService) {
  var vm = this;
  vm.sourceFilter = "";

  vm.views = [];

  vm.createNewView = function(){
    preparationService.loadView().then(function(){
      $location.path( "/editor" );
    });
  };

  vm.loadView = function(id){
    preparationService.loadView(id).then(function(){
      $location.path( "/editor" );
    });
  };

  vm.deleteView = function(id){
    preparationService.loadView(id);
    var viewName = preparationService.getName();
    preparationService.loadView();

    $uibModal.open({
      templateUrl: 'delete.html',
      controllerAs: "delete",
      controller: function($uibModalInstance, viewName, parent){
        var vm = this;
        vm.viewName = viewName;
        vm.ok = function(){
          preparationService.deleteView(id).then(function(){
            $uibModalInstance.close();
            parent.loadViews();
          });
        };
        vm.cancel = function(){
          $uibModalInstance.dismiss('cancel');
        };
      },
      resolve: {
        viewName: function () {
          return viewName;
        },
        parent: function() {
          return vm;
        }
      }
    });

  };

  vm.cloneView = function(id){
    preparationService.cloneView(id).then(function(){
      $location.path("/editor");
    });
  };

  vm.loadViews = function() {
    preparationService.getViews().then(function(views){
      vm.views = views;
    });
  }

  activate();

  function activate(){
    vm.loadViews();
  }
}

