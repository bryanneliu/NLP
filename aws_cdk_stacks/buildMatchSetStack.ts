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

const JobName = 'BuildMatchSetStack';

interface MarketplacePlanProps {
    marketplaceId: number;
    region: string;
    marketplace: string;
    match_set_on_date: string;
    latest_solr_index_date: string;
    daily_set_query_count: number;
    daily_tail_set_query_count: number;
    S3_dataset: string;
    require_tokenization: number;
    enable_added_token_stats: number;
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
        daily_set_query_count: 300000,
        daily_tail_set_query_count: 300000,
        S3_dataset: 's3://synonym-expansion-experiments/asin_level_synonym_expansion/title_dataset/uk/20221008/parquet/',
        require_tokenization: 0,
        enable_added_token_stats: 1,
        processingCoreInstanceNumber: 16,
        scheduleCron: Schedule.expression('cron(00 3,15 * * ? *)')
    }
}

export class BuildMatchSetEmrStack extends DeploymentStack {
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
                    stepName: 'Build-query-sets',
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
                        `${pyScriptBasePath}/measurement/match_set/1_build_query_set.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--match_set_on_date',
                        `\${match_set_on_date}`,
                        '--daily_set_query_count',
                        `\${daily_set_query_count}`,
                        '--daily_tail_set_query_count',
                        `\${daily_tail_set_query_count}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-2`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Normalize-query-sets',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/match_set/common.py`,
                        `${pyScriptBasePath}/measurement/match_set/2_tokenize_query_set.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--match_set_on_date',
                        `\${match_set_on_date}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-3`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Build-match-sets',
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
                        `${pyScriptBasePath}/measurement/match_set/3_build_match_set.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--match_set_on_date',
                        `\${match_set_on_date}`,
                        "--latest_solr_index_date",
                        `\${latest_solr_index_date}`
                    ],
                    retry: [
                        emrDataComputationDefaultRetryProps
                    ],
                    actionOnFailure: ActionOnFailure.CONTINUE
                },
                {
                    id: `${JobName}-Step-4`,
                    type: SfnTaskType.EmrAddStep,
                    stepName: 'Calculate_match_rate_improvement',
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
                        `${pyScriptBasePath}/utils/s3_utils.py,${pyScriptBasePath}/utils/marketplace_utils.py,${pyScriptBasePath}/measurement/match_set/common.py`,
                        `${pyScriptBasePath}/measurement/match_set/4_calculate_match_rate.py`,
                        '--marketplace_id',
                        `\${marketplaceId}`,
                        '--match_set_on_date',
                        `\${match_set_on_date}`,
                        "--S3_dataset",
                        `\${S3_dataset}`,
                        "--require_tokenization",
                        `\${require_tokenization}`,
                        '--enable_added_token_stats',
                        `\${enable_added_token_stats}`
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
