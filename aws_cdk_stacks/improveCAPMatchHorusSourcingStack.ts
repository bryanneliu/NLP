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
    EmrAsset,
    StageName
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
import { createImproveMatchEmrProfile} from '../config/emrProfiles';
import { StackInputProps } from '../common/stack';
import { JsonPath } from "aws-cdk-lib/aws-stepfunctions";
import {
    emrClusterDefaultRetryProps,
    emrDataComputationDefaultRetryProps
} from "../stepfunctions/retry-config";
import { dailyWorkflowTimeout } from "../stepfunctions/timeout-config";
import { Action } from 'aws-cdk-lib/aws-codepipeline';

const JobName = 'ImproveCAPMatchStack';

interface MarketplacePlanProps {
    marketplaceId: number;
    region: string;
    verbose_mode: number;
    rdd_partitions: number;
    review_count_threshold: number;
    tmp_output_prefix: string;
    output_prefix: string;
    ad_type: string;
    processingCoreInstanceNumber: number;
    scheduleCron: Schedule;
}

export const MarketplacePlan: { [key: string]: MarketplacePlanProps } = {
    us: {
        marketplaceId: 1,
        region: 'NA',
        verbose_mode: 0,
        rdd_partitions: 100,
        review_count_threshold: 10,
        tmp_output_prefix: 'dataset/weblab_improve_CAP_match/intermediate',
        output_prefix: 'dataset/weblab_improve_CAP_match/intermediate',
        processingCoreInstanceNumber: 32,
        ad_type: 'SPEAR3_1',
        scheduleCron: Schedule.expression('cron(00 6/8 * * ? *)'),
    }
}

export const OutputBuckets: {[key in StageName] : string} = {
    [StageName.DEV]: "spear-shared",
    [StageName.BETA]: "spear-beta",
    [StageName.PROD]: "spear-offline-data-prod"
}

export class ImproveCAPMatchEmrStack extends DeploymentStack {
    constructor(parent: App, name: string, props: StackInputProps) {
        super(parent, name, {
            softwareType: SoftwareType.INFRASTRUCTURE,
            env: props.env,
            stackName: props.stackName,
        });
        const stageName = props.deploymentGroup.name;
        const pyScriptBasePath = `${EmrAsset.getS3Path(this, stageName, EmrAssetNameCore)}/${EmrPyScriptLibPath}`;
        const outputBucket =  OutputBuckets[props.stageName]

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
                    emrProfile: createImproveMatchEmrProfile(`${stageName}-${JobName}`),
                    retry: [
                        emrClusterDefaultRetryProps
                    ],
                    autoTerminationPolicy: { idleTimeout: Duration.hours(3) }
                },
                {
                    id: `${JobName}-Step-1`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Prepare-Targeted-ASIN-Queries',
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
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/1_prepare_targeted_asin_queries.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
                },
                {
                    id: `${JobName}-Step-3`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Improve-Loose-Match',
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
                        '--executor-memory',
                        '64g',
                        '--jars',
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/3_improve_loose_match.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`,
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
                },
                {
                    id: `${JobName}-Step-4`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Improve-Exact-Match',
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
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/4_improve_exact_match.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`,
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
                },
                {
                    id: `${JobName}-Step-5`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Improve-Phrase-Match',
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
                        '--executor-memory',
                        '64g',
                        '--jars',
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/5_improve_phrase_match.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`,
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
                },
                {
                    id: `${JobName}-Step-6`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Improve-Broad-Match',
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
                        '--executor-memory',
                        '96g',
                        '--jars',
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py,${pyScriptBasePath}/improve_cap_match_horus/synonym.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/6_improve_broad_match.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`,
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
                },
                {
                    id: `${JobName}-Step-7`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Merge',
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
                        '--executor-memory',
                        '64g',
                        '--jars',
                        's3://spear-team/juanwj/query_demand/SourcingKeywordMatchingCore-1.0-super.jar',
                        '--py-files',
                        `${pyScriptBasePath}/utils/s3_with_nkw_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/improve_cap_match_horus/match_commons.py,${pyScriptBasePath}/improve_cap_match_horus/tokenizer_enhanced.py`,
                        `${pyScriptBasePath}/improve_cap_match_horus/7_merge.py`,
                        '--execute_date',
                        `\${executeDate}`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--region',
                        `\${region}`,
                        '--verbose_mode',
                        `\${verbose_mode}`,
                        '--rdd_partitions',
                        `\${rdd_partitions}`,
                        '--review_count_threshold',
                        `\${review_count_threshold}`,
                        '--janus_qdf_bucket',
                        `${OutputBuckets[StageName.PROD]}`,
                        '--tmp_output_bucket',
                        `${outputBucket}`,
                        '--output_bucket',
                        `${outputBucket}`,
                        '--tmp_output_prefix',
                        `\${tmp_output_prefix}`,
                        '--output_prefix',
                        `\${output_prefix}`,
                        '--ad_type',
                        `\${ad_type}`,
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ]
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
                    marketplace: marketplace,
                    marketplaceId: MarketplacePlan[marketplace].marketplaceId,
                    region: MarketplacePlan[marketplace].region,
                    verbose_mode: MarketplacePlan[marketplace].verbose_mode,
                    rdd_partitions: MarketplacePlan[marketplace].rdd_partitions,
                    review_count_threshold: MarketplacePlan[marketplace].review_count_threshold,
                    tmp_output_prefix: MarketplacePlan[marketplace].tmp_output_prefix,
                    output_prefix: MarketplacePlan[marketplace].output_prefix,
                    ad_type: MarketplacePlan[marketplace].ad_type,
                    processingCoreInstanceNumber: MarketplacePlan[marketplace].processingCoreInstanceNumber
                },
            });
        });
    }
}
