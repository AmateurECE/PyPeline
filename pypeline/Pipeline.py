###############################################################################
# NAME:             Pipeline.py
#
# AUTHOR:           Ethan D. Twardy <edtwardy@mtu.edu>
#
# DESCRIPTION:      Implements the logic of the pipeline.
#
# CREATED:          02/21/2021
#
# LAST EDITED:      02/21/2021
###

import argparse
from importlib import resources
import inspect

import yaml
import cerberus
from .Stage import Stage

# TODO: Add ability for multiple pipelines
# TODO: Add support for stage instances
# TODO: Add ability for parallel stages

def yamlLoader(configurationText):
    return yaml.load(configurationText, Loader=yaml.FullLoader)

class StageWrapper:
    def __init__(self, moduleName='', module=None, className='',
                 instance=None, consumer='', provider='', _class=None,
                 scriptName='', configName=''):
        self.module = module
        self._class = _class
        self.names = {'module': moduleName, 'class': className,
                      'script': scriptName, 'configName': configName}
        self.instance = instance
        self.consumer = consumer
        self.provider = provider

    def getProviders(self):
        return self._class.Providers

    def configure(self, configuration):
        try:
            if self.names['config'] not in configuration:
                return # No configuration for this module
            schema = yamlLoader(
                resources.read_text(self.names['module'], 'schema.yaml'))
            validator = cerberus.Validator(schema)
            validator.validate(configuration[self.names['config']])
        except FileNotFoundError:
            pass
        self.instance.configure(configuration[self.names['config']])
        return

    def instantiateConsumerOf(self, consumer):
        if not hasattr(self._class, 'Consumers') or \
           not hasattr(self._class, 'Providers') or \
           consumer not in self._class.Consumers:
            raise AttributeError(f'{self.names["class"]} does not have a '
                                 '"{consumer}" Consumer')
        self.instance = self._class()
        self.consumer = consumer
        if not isinstance(self.instance, Stage):
            className = self.names['class']
            raise AttributeError(f'{className} is not a Pipeline Stage')

    def setNames(self, moduleName):
        self.names = {
            'class': moduleName.split('.')[-1],
            'script': moduleName.split('.')[-2],
            'module': '.'.join(moduleName.split('.')[:-2]),
        }
        self.names['config'] = '.'.join([
            self.names['module'], self.names['script'], self.names['class']])


    def loadModule(self, moduleName):
        self.setNames(moduleName)
        self.module = __import__(
            self.names['module'] + '.' + self.names['script'],
            fromlist=[self.names['class']])
        self._class = getattr(self.module, self.names['class'])
        if not inspect.isclass(self._class):
            raise AttributeError(f'Class named {moduleName} not found')

class Pipeline:
    def __init__(self, config):
        self.pipeline = list()
        self.configure(config)

    def loadInitialStage(self, firstStage):
        firstClass = StageWrapper()
        firstClass.loadModule(firstStage)
        firstClass.instantiateConsumerOf('None')
        self.pipeline.append(firstClass)
        return firstClass

    def loadAdditionalStage(self, stage):
        # Verify provider of first class matches consumer of second class
        wrapper = StageWrapper()
        wrapper.loadModule(stage)
        for provider in self.pipeline[-1].getProviders():
            try:
                wrapper.instantiateConsumerOf(provider)
                self.pipeline[-1].provider = provider
                self.pipeline.append(wrapper)
                return wrapper
            except AttributeError:
                pass
        raise AttributeError(f'{stage} has no Consumer for precedents')

    def configure(self, config):
        # Read configuration
        if 'Pipeline' not in config or not config['Pipeline']:
            return

        # Construct pipeline, verifying consumers/providers of each stage
        # and configuration schema for stage
        self.loadInitialStage(config['Pipeline'][0]).configure(config)
        for stage in config['Pipeline'][1:]:
            self.loadAdditionalStage(stage).configure(config)
        if 'None' not in self.pipeline[-1].getProviders():
            raise AttributeError('Final stage does not implement "None"'
                                 ' provider')
        self.pipeline[-1].provider = 'None'

    def execute(self):
        output = None
        for stageWrapper in self.pipeline:
            instance = stageWrapper.instance
            instance.consume(stageWrapper.consumer, output)
            output = instance.provide(stageWrapper.provider)

###############################################################################
# Main
###

def readConfig(configurationFile, loader=yamlLoader):
    configuration = loader(''.join(configurationFile.readlines()))

    # Validate configuration file
    schema = yamlLoader(resources.read_text('pypeline', 'schema.yaml'))
    validator = cerberus.Validator(schema)
    validator.validate(configuration)
    return configuration

def main():
    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', help=('The Pipeline file'),
                        default='pipeline.yaml')
    arguments = vars(parser.parse_args())

    with open(arguments['file'], 'r') as configFile:
        pipeline = Pipeline(readConfig(configFile))
    pipeline.execute()

if __name__ == '__main__':
    main()

###############################################################################
