import { App, Duration } from 'aws-cdk-lib';
import { ActionOnFailure } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Schedule } from 'aws-cdk-lib/aws-events';
import {
    InfraCoreResources,
    SfnAlarm,
    SfnConfigProps,
    SfnScheduler,
    SfnTaskType,
    StepFunctions,
    EmrAsset
} from '@amzn/adsdelivery_offline_infrastructure_core';
import {
    DataSource,
    EmrAssetNameCore,
    EmrPyScriptLibPath,
    SpearSimProps,
    SpektrTommyQuDataDelay
} from '../config/constants';
import { DeploymentStack, SoftwareType } from '@amzn/pipelines';
import { CtiSpear, ResolverGroupNota } from '../stepfunctions/constants';
import { createMeasurementEmrProfile } from '../config/emrProfiles';
import { StackInputProps } from '../common/stack';
import { JsonPath } from "aws-cdk-lib/aws-stepfunctions";
import {
    emrClusterDefaultRetryProps,
    emrDataComputationDefaultRetryProps
} from "../stepfunctions/retry-config";
import { dailyWorkflowTimeout } from "../stepfunctions/timeout-config";

const JobName = 'BuildMatchSetForSolrTPSStack';

interface MarketplacePlanProps {
    marketplaceId: number;
    region: string;
    marketplace: string;
    match_set_on_date: string;
    latest_solr_index_date: string;
    S3_solrTPS: string;
    processingCoreInstanceNumber: number;
    scheduleCron: Schedule;
}

export const MarketplacePlan: { [key: string]: MarketplacePlanProps } = {
    uk: {
        marketplaceId: 3,
        region: 'EU',
        marketplace: 'UK',
        match_set_on_date: '20221211',
        latest_solr_index_date: '2022-12-12',
        S3_solrTPS: 's3://tps-diff-output/TPSGenCompareDiffOutput/asimmmahm-flatindex-na-fixed-5/192d62c6-f309-42f9-84dd-d6b8c7466e3b',
        processingCoreInstanceNumber: 16,
        scheduleCron: Schedule.expression('cron(00 3,15 * * ? *)')
    }
}

export class BuildMatchSetForSolrTPSEmrStack extends DeploymentStack {
    constructor(parent: App, name: string, props: StackInputProps) {
        super(parent, name, {
            softwareType: SoftwareType.INFRASTRUCTURE,
            env: props.env,
            stackName: props.stackName,
        });
        const stageName = props.deploymentGroup.name;
        const pyScriptBasePath = `${EmrAsset.getS3Path(this, stageName, EmrAssetNameCore)}/${EmrPyScriptLibPath}`;

        const sfnConfig: SfnConfigProps = {
            id: `${JobName}-Workflow-Worker`,
            stateMachineName: `${JobName}-Workflow-Worker`,
            tasks: [
                {
                    id: `${JobName}-Create-EmrStack-Emr-Cluster`,
                    type: SfnTaskType.EmrCreateCluster,
                    resourceName: InfraCoreResources.Emr,
                    clusterName: `${stageName}-${JobName}-Emr`,
                    clusterNamePath: "States.Format('" + `${stageName}-${JobName}-Emr` + "-MP-" + "{}', $.marketplaceId)",
                    coreInstanceNumber: JsonPath.numberAt('$.processingCoreInstanceNumber'),
                    emrProfile: createMeasurementEmrProfile(`${stageName}-${JobName}`),
                    retry: [
                        emrClusterDefaultRetryProps
                    ],
                    autoTerminationPolicy: { idleTimeout: Duration.hours(3) }
                },
                {
                    id: `${JobName}-Step-1`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Compare_solrTPS_match_rate',
                    args: [
                        'spark-submit',
                        '--master',
                        'yarn',
                        '--deploy-mode',
                        'client',
                        '--queue',
                        'default',
                        '--conf',
                        'spark.dynamicAllocation.enabled=true',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/match_set/common.py`,
                        `${pyScriptBasePath}/measurement/match_set/solrTPS_compare_match_rate.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--match_set_on_date',
                        `\${match_set_on_date}`,
                        "--latest_solr_index_date",
                        `\${latest_solr_index_date}`,
                        "--s3_solrTPS",
                        `\${S3_solrTPS}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Terminate-EmrStack-Emr`,
                    type: SfnTaskType.EmrTerminateCluster,
                    retry: [
                        emrClusterDefaultRetryProps
                    ]
                }
            ],
            timeout: dailyWorkflowTimeout
        };

        const stepFunction = new StepFunctions(this, `${stageName}-${JobName}-Workflow-Worker`, {
            deploymentGroup: props.deploymentGroup,
            sfnConfig: sfnConfig,
        });

        new SfnAlarm(this, `${JobName}-Alarm`, {
            deploymentGroup: props.deploymentGroup,
            stateMachine: stepFunction.stateMachine,
            alarmActions: [
                {
                    cti: CtiSpear,
                    resolverGroup: ResolverGroupNota,
                },
            ],
        });

        Object.keys(MarketplacePlan).forEach((marketplace) => {
            new SfnScheduler(this, `Schedule-${JobName}-${marketplace}`, {
                deploymentGroup: props.deploymentGroup,
                schedule: MarketplacePlan[marketplace].scheduleCron,
                stateMachineArns: [stepFunction.stateMachine.stateMachineArn],
                sfnInput: {
                    marketplaceId: MarketplacePlan[marketplace].marketplaceId,
                    region: MarketplacePlan[marketplace].region,
                    processingCoreInstanceNumber: MarketplacePlan[marketplace].processingCoreInstanceNumber
                },
            });
        });
    }
}
