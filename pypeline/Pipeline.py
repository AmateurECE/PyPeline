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

class Pipeline:
    def __init__(self):
        self.pipeline = list()

    @classmethod
    def verifyConsumer(cls, _class, name, attr):
        if not hasattr(_class, 'Consumers') or \
           not hasattr(_class, 'Providers') or \
           attr not in _class.Consumers:
            raise AttributeError(f'{name} does not have "{attr}" Consumer')
        instance = _class()
        if not isinstance(instance, Stage):
            raise AttributeError(f'{name} is not a Pipeline Stage')
        return instance

    @classmethod
    def loadClassOfModule(cls, moduleName):
        className = moduleName.split('.')[-1]
        moduleName = '.'.join(moduleName.split('.')[:-1])
        module = __import__(moduleName, fromlist=[className])
        _class = getattr(module, className)
        if not inspect.isclass(_class):
            raise AttributeError(f'Class named {moduleName} not found')
        return _class

    def loadStage(self, firstStage, secondStage=None):
        # TODO: Add configuration
        # TODO: Add ability for parallel stages
        firstClass = Pipeline.loadClassOfModule(firstStage)
        if not self.pipeline:
            # If not self.pipeline: Verify first class has 'None' consumer,
            instance = Pipeline.verifyConsumer(firstClass, firstStage, 'None')
            self.pipeline.append(['None', instance])

        # Verify provider of first class matches consumer of second class
        if not secondStage:
            return
        secondClass = Pipeline.loadClassOfModule(secondStage)
        for provider in self.pipeline[-1][1].Providers:
            try:
                instance = Pipeline.verifyConsumer(
                    secondClass, secondStage, provider)
                self.pipeline[-1].append(provider)
                self.pipeline.append([provider, instance])
                return
            except AttributeError:
                pass
        raise AttributeError(f'{secondStage} has no Consumer for {firstStage}')

    def configure(self, configurationFile, loader=yamlLoader):
        # Read configuration
        configuration = loader(''.join(configurationFile.readlines()))

        # Validate configuration file
        schema = yaml.load(resources.read_text('pypeline', 'schema.yaml'),
                           Loader=yaml.FullLoader)
        validator = cerberus.Validator(schema)
        validator.validate(configuration)

        # Construct pipeline, verifying consumers/providers of each stage
        # and configuration schema for stage
        if not configuration['Pipeline']:
            return

        if len(configuration['Pipeline']) == 1:
            self.loadStage(configuration['Pipeline'][0])
        else:
            for index in range(0, len(configuration['Pipeline']) - 1):
                firstStage = configuration['Pipeline'][index]
                secondStage = configuration['Pipeline'][index + 1]
                self.loadStage(firstStage, secondStage)
        if 'None' not in self.pipeline[-1][1].Providers:
            raise AttributeError('Final stage does not implement "None"'
                                 ' provider')
        self.pipeline[-1].append('None')

    def execute(self):
        output = None
        for stageTuple in self.pipeline:
            consumer, stage, provider = stageTuple
            stage.consume(consumer, output)
            output = stage.provide(provider)

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
