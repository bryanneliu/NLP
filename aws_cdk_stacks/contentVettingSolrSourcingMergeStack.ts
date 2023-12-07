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
import { createCAPExpansionEmrProfile } from '../config/emrProfiles';
import { StackInputProps } from '../common/stack';
import { JsonPath } from "aws-cdk-lib/aws-stepfunctions";
import {
    emrClusterDefaultRetryProps,
    emrDataComputationDefaultRetryProps
} from "../stepfunctions/retry-config";
import { dailyWorkflowTimeout } from "../stepfunctions/timeout-config";

const JobName = 'SolrContentVettingMerge';

interface MarketplacePlanProps {
    marketplaceId: number;
    region: string;
    solr_index_latest_ds: string;
    head_asin_expansion_count_threshold: number;
    head_asin_capped_token_count: number;
    non_head_asin_capped_token_count: number;
    processingCoreInstanceNumber: number;
    scheduleCron: Schedule;
}

export const MarketplacePlan: { [key: string]: MarketplacePlanProps } = {
    us: {
        marketplaceId: 1,
        region: 'NA',
        solr_index_latest_ds: '2022-12-11',
        head_asin_expansion_count_threshold: 200,
        head_asin_capped_token_count: 50,
        non_head_asin_capped_token_count: 30,
        processingCoreInstanceNumber: 32,
        scheduleCron: Schedule.expression('cron(00 0,12 * * ? *)'),
    }
}

export class SolrContentVettingMergeEmrStack extends DeploymentStack {
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
                    emrProfile: createCAPExpansionEmrProfile(`${stageName}-${JobName}`),
                    retry: [
                        emrClusterDefaultRetryProps
                    ],
                    autoTerminationPolicy: { idleTimeout: Duration.hours(3) }
                },
                {
                    id: `${JobName}-Step-1`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Merge-Multiple-Versions',
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
                        '--jars',
                        's3://spear-team/shared/jars/solr8norm/AdsDeliveryLatacSparkUtil-1.0.jar,s3://spear-team/shared/jars/solr8norm/AmazonClicksTextProcessing-1.2-standalone.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/solr_content_vetting/common.py`,
                        `${pyScriptBasePath}/solr_content_vetting/merge_multiple_versions.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--solr_index_latest_ds',
                        `\${solr_index_latest_ds}`,
                        '--head_asin_expansion_count_threshold',
                        `\${head_asin_expansion_count_threshold}`,
                        '--head_asin_capped_token_count',
                        `\${head_asin_capped_token_count}`,
                        '--non_head_asin_capped_token_count',
                        `\${non_head_asin_capped_token_count}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-2`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Prepare-Region-CSV-data',
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
                        `${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/solr_content_vetting/common.py`,
                        `${pyScriptBasePath}/solr_content_vetting/prepare_region_csv_data.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`
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
