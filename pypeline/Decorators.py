###############################################################################
# NAME:             Decorators.py
#
# AUTHOR:           Ethan D. Twardy <edtwardy@mtu.edu>
#
# DESCRIPTION:      Decorate the Pipeline stages
#
# CREATED:          02/21/2021
#
# LAST EDITED:      02/21/2021
###

import logging

def pipelineStage(_class):
    if not hasattr(_class, 'Consumers'):
        _class.Consumers = dict()
    if not hasattr(_class, 'Providers'):
        _class.Providers = dict()
    for name in _class.__dict__:
        method = _class.__dict__[name]
        if hasattr(method, 'consumerOf'):
            if method.consumerOf in _class.Consumers:
                logging.warning('%s has already defined %s as a Consumer',
                               _class.__name__, name)
            _class.Consumers[method.consumerOf] = method
        if hasattr(method, 'providerOf'):
            if method.providerOf in _class.Providers:
                logging.warning('%s has already defined %s as a Provider',
                               _class.__name__, name)
            _class.Providers[method.providerOf] = method
    return _class

def consumes(consumerType):
    def decoratorConsumes(func):
        func.consumerOf = consumerType
        return func
    return decoratorConsumes

def provides(providerType):
    def decoratorProvides(func):
        func.providerOf = providerType
        return func
    return decoratorProvides

###############################################################################
