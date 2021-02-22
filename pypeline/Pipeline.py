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

def yamlLoader(configurationText):
    return yaml.load(configurationText, Loader=yaml.FullLoader)

class StageWrapper:
    def __init__(self, moduleName='', module=None, className='',
                 instance=None, consumer='', provider='', _class=None,
                 scriptName=''):
        self.module = {'object': module, 'name': moduleName}
        self._class = {'object': _class, 'name': className}
        self.scriptName = scriptName
        self.instance = instance
        self.consumer = consumer
        self.provider = provider

    def getProviders(self):
        return self._class['object'].Providers

    def configure(self, configuration):
        try:
            configName = '.'.join([self.module['name'], self.scriptName,
                                   self._class['name']])
            if configName not in configuration:
                return # No configuration for this module
            schemaDoc = resources.read_text(self.module['name'], 'schema.yaml')
            schema = yamlLoader(schemaDoc)
            validator = cerberus.Validator(schema)
            validator.validate(configuration[configName])
        except FileNotFoundError:
            pass
        self.instance.configure(configuration[configName])
        return

    def instantiateConsumerOf(self, consumer):
        if not hasattr(self._class['object'], 'Consumers') or \
           not hasattr(self._class['object'], 'Providers') or \
           consumer not in self._class['object'].Consumers:
            raise AttributeError(f'{self._class["name"]} does not have a '
                                 '"{consumer}" Consumer')
        self.instance = self._class['object']()
        self.consumer = consumer
        if not isinstance(self.instance, Stage):
            className = self._class['name']
            raise AttributeError(f'{className} is not a Pipeline Stage')

    def loadModule(self, moduleName):
        self._class['name'] = moduleName.split('.')[-1]
        self.scriptName = moduleName.split('.')[-2]
        self.module['name'] = '.'.join(moduleName.split('.')[:-2])
        self.module['object'] = __import__(
            self.module['name'] + '.' + self.scriptName,
            fromlist=[self._class['name']])
        self._class['object'] = getattr(
            self.module['object'], self._class['name'])
        if not inspect.isclass(self._class['object']):
            raise AttributeError(f'Class named {moduleName} not found')

class Pipeline:
    def __init__(self):
        # TODO: RAII
        self.pipeline = list()

    def loadInitialStage(self, firstStage):
        firstClass = StageWrapper()
        firstClass.loadModule(firstStage)
        firstClass.instantiateConsumerOf('None')
        self.pipeline.append(firstClass)
        return firstClass

    def loadAdditionalStage(self, stage):
        # TODO: Add ability for parallel stages
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

    @classmethod
    def readConfig(cls, configurationFile, loader=yamlLoader):
        configuration = loader(''.join(configurationFile.readlines()))

        # Validate configuration file
        schema = yamlLoader(resources.read_text('pypeline', 'schema.yaml'))
        validator = cerberus.Validator(schema)
        validator.validate(configuration)
        return configuration

    def configure(self, configurationFile, loader=yamlLoader):
        # Read configuration
        config = Pipeline.readConfig(configurationFile, loader)
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

def getArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', help=('The Pipeline file'),
                        default='pipeline.yaml')
    return vars(parser.parse_args())

def main():
    arguments = getArguments()
    pipeline = Pipeline()
    with open(arguments['file'], 'r') as configurationFile:
        pipeline.configure(configurationFile)
    pipeline.execute()

if __name__ == '__main__':
    main()

###############################################################################
