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

const JobName = 'AnalyzeOfflineSolrStack';

interface MarketplacePlanProps {
    marketplaceId: number;
    s3_indexable: string;
    s3_janus: string;
    task: string;
    processingCoreInstanceNumber: number;
    scheduleCron: Schedule;
}

export const MarketplacePlan: { [key: string]: MarketplacePlanProps } = {
    us: {
        marketplaceId: 3,
        s3_indexable: 's3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp_1/indexable-json/adVersion=1693760188203/',
        s3_janus: 's3://adsdelivery-index-na-master/Master2/indexables/coldstart/sp_1/janus-json/adVersion=1693760188203/',
        task: 'content_vetting',
        processingCoreInstanceNumber: 64,
        scheduleCron: Schedule.expression('cron(00 3,15 * * ? *)')
    }
}

export class AnalyzeOfflineSolrEmrStack extends DeploymentStack {
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
                    stepName: 'Analyze_content_vetting',
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
                        `${pyScriptBasePath}/measurement/solr_offline_analysis/schemas.py`,
                        `${pyScriptBasePath}/measurement/solr_offline_analysis/1_analyze_content_vetting_by_parse_solr_indexable.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--s3_janus',
                        `\${s3_janus}`,
                        '--s3_indexable',
                        `\${s3_indexable}`,
                        '--task',
                        `\${task}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-2`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Analyze_index_distribution',
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
                        `${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/solr_offline_analysis/schemas.py`,
                        `${pyScriptBasePath}/measurement/solr_offline_analysis/2_analyze_index_distribution.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--s3_indexable',
                        `\${s3_indexable}`,
                        '--s3_janus',
                        `\${s3_janus}`,
                        '--task',
                        `\${task}`
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
                    processingCoreInstanceNumber: MarketplacePlan[marketplace].processingCoreInstanceNumber
                },
            });
        });
    }
}
