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
import {dailyWorkflowTimeout, weeklyWorkflowTimeout} from "../stepfunctions/timeout-config";

const JobName = 'WeblabAnalysisStack';

interface MarketplacePlanProps {
    marketplaceId: number;
    start_date: string;
    end_date: string;
    weblab_name: string;
    feed_glue_table: string;
    Solr_or_Horus: string;
    ad_type: string;
    processingCoreInstanceNumber: number;
    compare_treatments: string;
    token_dataset: string;
    token_column: string;
}

export const MarketplacePlan: { [key: string]: MarketplacePlanProps } = {
    us: {
        marketplaceId: 1,
        start_date: '2023-01-21',
        end_date: '2023-01-22',
        weblab_name: 'SPONSORED_PRODUCTS_578419',
        feed_glue_table: 'spear_yujiehe.spear_weblab_logging_multipleids_sbm',
        Solr_or_Horus: 'Horus',
        ad_type: 'SPEAR1_1',
        processingCoreInstanceNumber: 16,
        compare_treatments: 'T1,T3',
        token_dataset: 's3://spear-team/test_dataset/',
        token_column: 'bulletPoints'
    }
}

export class WeblabAnalysisEmrStack extends DeploymentStack {
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
                    stepName: 'Calculate-Overall-Metrics',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/1.overall_metrics.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-2`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Daily-Weblab-Join-Janus',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/2.daily_weblab_join_janus.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-3`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Keyword_match_type_metrics',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/3.keyword_match_type_metrics.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-4`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Performance_by_segment',
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
                        '--conf',
                        'spark.kryoserializer.buffer.max=1g',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/4.performance_by_segment.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-5`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'AdType_metrics',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/5.ad_type_metrics.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-6`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Cold_ASIN_Metrics',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/6.cold_asins_metrics.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${end_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-7`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Generate treatment comparison dataset for Day 1',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/7.overlap_analysis_metrics.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        // Only running for Day 1. Can be overriden as needed for different days
                        `\${start_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`,
                        '--treatments',
                        `\${compare_treatments}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-8`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Solr - Generate new dataset performance',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/weblab_analysis/common.py`,
                        `${pyScriptBasePath}/measurement/weblab_analysis/solr/8.dataset_asin_analysis.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--start_date',
                        `\${start_date}`,
                        '--end_date',
                        `\${start_date}`,
                        '--weblab_name',
                        `\${weblab_name}`,
                        '--feed_glue_table',
                        `\${feed_glue_table}`,
                        '--Solr_or_Horus',
                        `\${Solr_or_Horus}`,
                        '--ad_type',
                        `\${ad_type}`,
                        '--treatments',
                        `\${compare_treatments}`,
                        '--token_dataset',
                        `\${token_dataset}`,
                        '--token_column',
                        `\${token_column}`
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
            timeout: weeklyWorkflowTimeout
        };

        const stepFunction = new StepFunctions(this, `${stageName}-${JobName}-Workflow-Worker`, {
            deploymentGroup: props.deploymentGroup,
            sfnConfig: sfnConfig,
        });

    }
}
