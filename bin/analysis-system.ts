#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { AnalysisSystemStack } from '../lib/analysis-system-stack';
import { dataSourceStack } from '../lib/data-source-stack';
import { SampleDataSourceStack } from '../lib/sample-data-source-stack';
import {config} from '../config/dev';


const app = new cdk.App();

const dataStack = config.isExistDB ?
    //  Use existing database
    new dataSourceStack(app, 'dataSourceStack',{
        env: { account: config.account, region: config.region },
        rdsIds: {
            dbClusterId: config.dbClusterId,
            secretId: config.secretId
        },
        vpcId: config.vpcId,
        dbPort:config.dbPort,
        dbName:config.dbName,
        dbHostname:config.dbHostname
    }):
    // Use Sample database
    new SampleDataSourceStack(app, 'SampleDataSourceStack');

new AnalysisSystemStack(app, 'AnalysisSystemStack', {
    vpc: dataStack.vpc,
    dbName: dataStack.dbName,
    dbPort: dataStack.dbPort,
    dbHostname: dataStack.dbHostname,
    dbSecret: dataStack.dbSecret
});