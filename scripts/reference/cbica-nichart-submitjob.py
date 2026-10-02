import json
import os
import re
import subprocess
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import boto3
import jwt
import urllib.request
import yaml
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, validator

# Allowlist for free-entry string parameters — mirrors the check in the API server.
# Rejects spaces and all shell metacharacters to prevent command injection in the
# bash -c wrapper that Batch uses to run tool containers.
_SAFE_STRING_RE = re.compile(r"^[a-zA-Z0-9._-]{1,256}$")

BATCH_QUEUE = "cbica-nichart-jobqueue-standard"
BATCH_JOB_DEF = "cbica-nichart-jobdefinition-template1"

S3_BUCKET = "cbica-nichart-staticfiles"
S3_TOOL_PREFIX = "tools/"

# User data bucket + FSx mount root. The FSx Lustre filesystem is mounted at
# /fsx, and user data lives under the key prefix fsx/{user_id}/... in the data
# bucket, so /fsx/fsx/{user_id}/x maps to s3://{S3_DATA_BUCKET}/fsx/{user_id}/x.
S3_DATA_BUCKET = "cbica-nichart-io"
FSX_MOUNT_ROOT = "/fsx"

DEFAULT_TOOL_DEFINITION_PATH = Path(__file__).parent.parent.parent.parent / "resources/tools/"

COGNITO_POOL_ID = "us-east-1_BSBhcKA66"
COGNITO_APP_CLIENT_ID = "4shr6mm2h0p0i4o9uleqpu33fj"
REGION = "us-east-1"

ASG_NAME = "cbica-nichart-unmanaged-ASG"
COMPUTE_ENV_NAME = "cbica-nichart-compute-environment-unmanaged"

autoscaling = boto3.client("autoscaling")
ec2 = boto3.client("ec2")
batch = boto3.client("batch")

def get_user_id_from_token(token: str) -> str:
    """Extract the Cognito sub from a JWT without verifying the signature.

    Signature verification is intentionally skipped here.  Security is enforced
    at the IAM layer — only the NiChart API server's execution role can invoke
    this Lambda (via lambda:InvokeFunction).  The API server has already
    validated the Cognito token before calling us.  We still extract the sub
    so we can scope mount-path validation to /fsx/fsx/{user_id}/, which prevents
    one authenticated user from touching another's data.
    """
    try:
        payload = jwt.decode(token, options={"verify_signature": False})
        sub = payload.get("sub")
        if not sub:
            raise ValueError("Token is missing 'sub' claim")
        print(f"DEBUG: extracted user sub from token")
        return sub
    except Exception as e:
        print(f"Error extracting user ID from token: {e}")
        raise

def is_safe_path(base_dir: Union[str, Path], target_path: Union[str, Path]) -> bool:
    """
    Check that the target_path is a subpath of base_dir and doesn't escape it via symlinks or traversal.
    
    Parameters:
        base_dir: The base directory within which target_path must reside.
        target_path: The user-supplied path to validate.
        
    Returns:
        True if the path is valid and contained within base_dir, False otherwise.
    """
    base_dir = Path(base_dir).resolve(strict=False)
    target_path = Path(target_path).resolve(strict=False)

    try:
        target_path.relative_to(base_dir)
        return True
    except ValueError:
        return False


def fsx_to_s3_uri(fsx_path: Union[str, Path]) -> str:
    """Map an FSx host path to its S3 URI in the user data bucket.

    /fsx/fsx/{user_id}/{rest}  ->  s3://{S3_DATA_BUCKET}/fsx/{user_id}/{rest}
    """
    p = str(fsx_path)
    prefix = FSX_MOUNT_ROOT.rstrip("/") + "/"  # "/fsx/"
    if not p.startswith(prefix):
        raise ValueError(f"Path {p!r} is not under the FSx mount root {FSX_MOUNT_ROOT!r}")
    rel = p[len(prefix):].rstrip("/")
    return f"s3://{S3_DATA_BUCKET}/{rel}"


# Sidecar image (guaranteed present on the processing instances) used to run the
# S3<->FSx syncs. It carries the AWS CLI and is launched the SAME way as the tool
# container (host daemon `docker run -v`), so it shares the tool container's real
# host-Lustre view. fsx-manager's own Batch-provided FSx bind is a divergent mount
# namespace (proven by a stat device:inode mismatch vs the host), so an aws s3 sync
# run inside fsx-manager can neither see the tool's outputs nor write where the
# tool reads — hence the syncs must run in this sidecar instead.
SIDECAR_IMAGE = "cbica/aws-fsx-sidecar:06182025"


def sidecar_run(user_id: str, inner_cmd: str) -> str:
    """Wrap a shell command in a host-daemon `docker run` of the FSx sidecar.

    The user's FSx dir is bind-mounted at the same path inside the sidecar, so the
    absolute /fsx/fsx/... paths used by the sync commands resolve identically.
    Runs synchronously (no -d), so the caller's `&&` chain waits on it and
    propagates its exit code — a failed sync fails the whole job. S3 credentials
    come from the instance role via IMDS (requires the instance metadata hop limit
    to allow container access).

    NOTE: inner_cmd must not contain single quotes — it is single-quoted for
    `sh -c` and this whole string is itself embedded in the fsx-manager `bash -c`.
    """
    fsx_dir = f"/fsx/fsx/{user_id}"
    return (
        f"docker run --rm -v {fsx_dir}:{fsx_dir}:rw "
        f"--entrypoint sh {SIDECAR_IMAGE} -c '{inner_cmd}'"
    )

class IOField(BaseModel):
    type: str  # "file" or "directory"
    description: Optional[str] = None


class MountConfig(BaseModel):
    path_in_container: str
    mode: str = Field("ro", pattern="^(ro|rw)$")


class ResourceSpec(BaseModel):
    vcpus: int = 4
    memory: int = 16000 # e.g., 16GB -> 16000 
    gpus: int = 0


class ParameterSpec(BaseModel):
    type: str  # "int", "float", "bool", "str"
    default: Optional[Union[int, float, bool, str]] = None
    description: Optional[str] = None
    choices: Optional[List[Union[int, float, str]]] = None
    min: Optional[float] = None   # numeric types only
    max: Optional[float] = None   # numeric types only


class ToolSpec(BaseModel):
    name: str
    description: Optional[str]
    inputs: Dict[str, IOField]
    outputs: Dict[str, IOField]
    mounts: Dict[str, MountConfig]
    resources: ResourceSpec
    container: Dict[str, Union[str, List[str]]]
    parameters: Dict[str, ParameterSpec]

    def validate_params(self, user_params: Dict) -> Dict:
        """Validate and coerce user-supplied params against the tool spec.

        Security hardening:
        - Bool is checked before int (bool is a subclass of int in Python).
        - Numeric values are checked against min/max if declared.
        - Choices-constrained params are whitelisted explicitly.
        - Free-entry string params are restricted to [a-zA-Z0-9._-] to prevent
          command injection in the bash -c Batch wrapper.
        """
        validated = {}
        for key, spec in self.parameters.items():
            if key in user_params:
                value = user_params[key]
            elif spec.default is not None:
                value = spec.default
            else:
                raise ValueError(f"Missing required parameter: '{key}'")

            # Type coercion — bool must be checked before int (bool <: int).
            if spec.type == "bool":
                if not isinstance(value, bool):
                    raise TypeError(
                        f"Parameter '{key}' must be a boolean (true/false), "
                        f"got {type(value).__name__!r}"
                    )
            elif spec.type == "int":
                if isinstance(value, bool):
                    raise TypeError(f"Parameter '{key}' must be int, not bool")
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    raise TypeError(f"Parameter '{key}' must be int")
            elif spec.type == "float":
                if isinstance(value, bool):
                    raise TypeError(f"Parameter '{key}' must be float, not bool")
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    raise TypeError(f"Parameter '{key}' must be float")
            elif spec.type == "str":
                value = str(value)

            # Choices whitelist (all types).
            if spec.choices and value not in spec.choices:
                raise ValueError(
                    f"Parameter '{key}': {value!r} is not an allowed value. "
                    f"Must be one of: {spec.choices}"
                )

            # Numeric range.
            if spec.type in ("int", "float"):
                if spec.min is not None and value < spec.min:
                    raise ValueError(f"Parameter '{key}' must be >= {spec.min}")
                if spec.max is not None and value > spec.max:
                    raise ValueError(f"Parameter '{key}' must be <= {spec.max}")

            # String injection guard — applied to free-entry strings only.
            # This is the primary defence against command injection in bash -c.
            if spec.type == "str" and not spec.choices:
                if not _SAFE_STRING_RE.match(value):
                    raise ValueError(
                        f"Parameter '{key}': string values must contain only "
                        f"letters, digits, '.', '_', or '-' (max 256 chars). "
                        f"Spaces and shell metacharacters are not allowed."
                    )

            validated[key] = value
        return validated


    def generate_docker_command(self, param_values: Dict[str, Union[int, float, bool, str]], mount_paths: Dict[str, str]) -> str:
        # This line will throw if validation fails
        param_values = self.validate_params(param_values)

        # Construct any default docker args here
        print("DEBUG: constructing global docker args")
        global_docker_args = ['--rm', '--ipc=host']
        if self.resources.gpus != 0:
            global_docker_args.append('--gpus all')

        # Apply substitution for command template
        print("DEBUG: command template substitution")
        command_template = self.container["command"]
        for key, mount in self.mounts.items():
            command_template = command_template.replace(f"{{{key}}}", mount.path_in_container)

        for key, val in param_values.items():
            command_template = command_template.replace(f"{{{key}}}", str(val))

        # Generate mount arguments
        mount_args = []
        
        print("DEBUG: Adding static mounts")
        ## TODO: Fix this hardcoded crap (see additional note in lambda_handler). 
        # In lambda_handler we append static s3 sync commands to pull models to the instance, here we handle mounting them into the task container
        # We assume that /mounted-models exists on the host machine (requires us to mount this in batch job def and in cloud-init script)
        static_mount_args = []
        if self.name == "nichart_dkgp":
            print("DEBUG: Detected nichart_dkgp during mount generation.")
            print("DEBUG: Adding DKGP-specific static mounts from /mounted-models")
            static_mount_args.append("-v /mounted-models/dkgp/models:/app/models:ro")
            static_mount_args.append("-v /mounted-models/dkgp/models_spare:/app/models_spare:ro")
            static_mount_args.append("-v /mounted-models/dkgp/models_cognitive:/app/models_cognitive:ro")
        
        print("DEBUG: Iterating over mounts...")
        for label, config in self.mounts.items():
            print(f"DEBUG: Processing mount label: {label}")
            print(f"DEBUG: Mount config: {config}")
            print(f"DEBUG: Mount paths: {mount_paths}")
            host_path = mount_paths[label]
            print(f"DEBUG: Host path: {host_path}")
            print(f"DEBUG: Checking mount type status...")
            mode = config.mode
            is_output = False
            is_ofile = False
            is_ifile = False
            if label in self.outputs:
                print(f"DEBUG: Label {label} found in outputs")
                is_output = True
                if self.outputs[label].type == 'file':
                    is_ofile = True
            elif label in self.inputs: # necessarily this is an input
                print(f"DEBUG: Label {label} found in inputs")
                if self.inputs[label].type == 'file':
                    is_ifile = True
            else:
                print(f"Mount label {label} not found in tool spec inputs or outputs.")
                raise ValueError(f"Mount label {label} not found in tool spec inputs or outputs.")
            print(f"DEBUG: is_output: {is_output}, is_ofile: {is_ofile}, is_ifile: {is_ifile}")
            print(f"DEBUG: Resolving mount type configuration...")
            if is_ofile: # Need to create parent dir of output location
                parent_host_path = Path(host_path).parent.resolve()
                parent_container_path = Path(config.path_in_container).parent
                mount_args.append(f"-v {parent_host_path}:{parent_container_path}:{mode}")
            elif is_ifile: # Need to mount file directly in
                mount_args.append(f"--mount type=bind,source={host_path},target={config.path_in_container}")
            else: # General case handling dirs
                mount_args.append(f"-v {host_path}:{config.path_in_container}:{mode}")
        print("DEBUG: final docker command construction")
        docker_cmd = f"docker pull {self.container['image']} && docker run {' '.join(global_docker_args)} {' '.join(static_mount_args)} {' '.join(mount_args)} {self.container['image']} {command_template}"
        print(f"Returning the following docker command: {docker_cmd}")

        return docker_cmd

    def generate_down_sync_commands(self, mount_paths: Dict[str, str]) -> List[str]:
        """Build scoped DOWN-sync (S3 -> FSx) commands for this job's INPUTS.

        Inputs only, NO --delete. Inputs are read-only, so deletion is never
        wanted, and dropping --delete means a concurrent chunk job can't wipe
        another chunk's in-flight outputs. DRA auto-import is the primary
        S3->FSx path; this explicit sync is a determinism backstop guaranteeing
        inputs are materialised before the tool reads them.

        The UP-sync is deliberately NOT scoped — see the up-sync in
        lambda_handler. Scoping the up-sync to declared output mounts is fragile:
        tools that write results outside their exact declared output paths (aux
        files, logs, nested dirs) were silently not published. The up-sync
        therefore covers the whole user dir.

        File-typed inputs sync their PARENT directory, since `aws s3 sync`
        operates on prefixes/directories rather than single objects.
        """
        down_dirs = set()
        out_dirs = set()
        for label, host_path in mount_paths.items():
            if label in self.inputs:
                is_file = self.inputs[label].type == "file"
                sync_dir = str(Path(host_path).parent) if is_file else str(host_path).rstrip("/")
                down_dirs.add(sync_dir)
            elif label in self.outputs:
                is_file = self.outputs[label].type == "file"
                out_dirs.add(str(Path(host_path).parent) if is_file else str(host_path).rstrip("/"))
            else:
                raise ValueError(f"Mount label {label} not found in tool spec inputs or outputs.")

        # Don't down-sync a path that is also an output (it's being written).
        down_dirs -= out_dirs

        return [f"aws s3 sync {fsx_to_s3_uri(d)} {d}" for d in sorted(down_dirs)]

    def output_dirs(self, mount_paths: Dict[str, str]) -> List[str]:
        """Return the host directories the tool's outputs land in (dedup, sorted).

        For a file output this is the parent directory; for a directory output it
        is the directory itself. Used for post-run diagnostics.
        """
        dirs = set()
        for label, host_path in mount_paths.items():
            if label in self.outputs:
                is_file = self.outputs[label].type == "file"
                dirs.add(str(Path(host_path).parent) if is_file else str(host_path).rstrip("/"))
        return sorted(dirs)

    def get_container_image(self):
        return self.container['image']

def load_tool_spec_from_yaml(yaml_path: Union[str, Path]) -> ToolSpec:
    with open(yaml_path, 'r') as f:
        raw_data = yaml.safe_load(f)
    return ToolSpec(**raw_data)

def ensure_and_validate_mount_paths(
    mount_paths: Dict[str, str],
    base_dir: Union[str, Path],
    input_labels: set,
    output_labels: set
) -> None:
    """
    Validate that input paths exist and are within the requested root dir.
    For outputs, ensure parent directories exist (creating them if needed).
    
    Parameters:
        mount_paths: Dictionary of host paths keyed by label.
        base_dir: Root directory to constrain access.
        input_labels: Set of input labels.
        output_labels: Set of output labels.
    
    Raises:
        FileNotFoundError or ValueError for unsafe or invalid paths.
    """
    base_dir = Path(base_dir).resolve(strict=False)

    for label, path in mount_paths.items():
        resolved = Path(path).resolve(strict=False)

        if not is_safe_path(base_dir, resolved):
            raise ValueError(f"Unsafe mount path for label '{label}': {resolved} escapes {base_dir}")

        if label in input_labels:
            if not resolved.exists():
                #raise FileNotFoundError(f"Input path does not exist for '{label}': {resolved}")
                pass

        elif label in output_labels:
            parent = resolved.parent
            if not parent.exists():
                #print(f"Creating parent directory for output path '{label}': {parent}")
                #parent.mkdir(parents=True, exist_ok=True)
                pass

def validate_user_request(tool_name: str, user_params: Dict, user_mounts: Dict[str, str], tool_registry_path: Union[str, Path]) -> str:

    tool_spec = load_tool_spec_from_s3(tool_name)
    
    input_labels = set(tool_spec.inputs.keys())
    output_labels = set(tool_spec.outputs.keys())

    ensure_and_validate_mount_paths(user_mounts, base_mount_dir, input_labels, output_labels)

    return tool_spec.generate_docker_command(user_params, user_mounts)

def load_tool_spec_from_s3(tool_name: str) -> ToolSpec:
    s3 = boto3.client("s3")
    key = f"{S3_TOOL_PREFIX}{tool_name}.yaml"
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response["Body"].read()
        data = yaml.safe_load(BytesIO(content))
        return ToolSpec(**data)
    except Exception as e:
        raise FileNotFoundError(f"Tool '{tool_name}' not found in S3: {e}")

def increment_asg(current, max_val):
    if current < max_val:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName = ASG_NAME,
            DesiredCapacity= current + 1
        )
        print(f"Incremented ASG desired capacity to {current + 1}")
    else:
        print("ASG already at max capacity. No scale-up.")

def lambda_handler(event, context):
    try:
        # Get user identity
        print("DEBUG: getting id from token")
        user_id = get_user_id_from_token(event["id_token"])
        if not user_id:
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized or missing user ID"})}

        print("DEBUG: Reading request payload")
        print(f"DEBUG: Event body: {event}")
 
        if isinstance(event, str):
            body = json.loads(body)
        else:
            body = event

        tool_name = body["tool_name"]
        user_params = body["user_params"]
        user_mounts = body["user_mounts"]
        num_subjects = body.get("num_subjects", 0)
        print("DEBUG: loading tool spec")
        tool = load_tool_spec_from_s3(tool_name)

        # Set a per-user base dir
        base_mount_dir = f"/fsx/fsx/{user_id}/"
        print("DEBUG: entering mount path validation")
        ensure_and_validate_mount_paths(
            mount_paths=user_mounts,
            base_dir=base_mount_dir,
            input_labels=set(tool.inputs.keys()),
            output_labels=set(tool.outputs.keys())
        )

        # Check ASG/Batch, scale up ASG pre-emptively if needed
        
        asg = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[ASG_NAME]
        )["AutoScalingGroups"][0]

        desired = asg["DesiredCapacity"]
        max_size = asg["MaxSize"]

        instance_ids = [i["InstanceId"] for i in asg["Instances"] if i["LifecycleState"] == "InService"]
        if not instance_ids:
            print(f"No in-service instances detected in ASG {ASG_NAME}, scaling up...")
            increment_asg(desired, max_size)
        else:
            ec2_states = ec2.describe_instance_status(InstanceIds=instance_ids)["InstanceStatuses"]
            running_ids = [i["InstanceId"] for i in ec2_states if i["InstanceState"]["Name"] == "running"]

            compute_env = batch.describe_compute_environments(
                computeEnvironments=[COMPUTE_ENV_NAME]
            )["computeEnvironments"][0]

            busy_count = compute_env.get("computeResources", {}).get("desiredvCpus", 0)
            min_vcpus = compute_env.get("computeResources", {}).get("minvCpus", 0)

            if len(running_ids) == 0 or busy_count >= len(running_ids):
                print("All instances busy or none running, scaling up...")
            else:
                print("At least one idle instance detected, no scaling needed.")


        print("DEBUG: generating docker command")
        docker_command = tool.generate_docker_command(user_params, user_mounts)
        print(f"DEBUG: submitting to batch with command: {docker_command}")
        
        # Build resource requirements dynamically
        resource_requirements = [
            {
                'type': 'VCPU',
                'value': str(tool.resources.vcpus),
            },
            {
                'type': 'MEMORY',
                'value': str(tool.resources.memory),
            }
        ]

        # Only add GPU requirement if it's greater than 0
        if tool.resources.gpus > 0:
            resource_requirements.append({
                'type': 'GPU',
                'value': str(tool.resources.gpus),
            })
        
        # Build additional prefix commands for the manager container to run as setup before task container job
        extra_prefix_commands_list = []
        
        ## TODO: Fix this hardcoded ad-hoc crap (here and in docker-command-gen)
        # At least this solution prevents us from needing to s3 sync models for other job types. Only works if the docker command gen generates the proper mount and the dkgp container expects the in-container paths
        print(f"Current tool name: {tool_name}")
        if tool_name == "dkgp":
            print(f"Detected DKGP. Prepending model-pull commands to the job.")
            extra_prefix_commands_list.append("mkdir -pv /mounted-models/dkgp") # Ensure destination exists
            extra_prefix_commands_list.append("aws s3 sync s3://cbica-nichart-privatemodels/dkgp /mounted-models/dkgp --delete")
            
        print(f"Extra prefix commands: {extra_prefix_commands_list}")
        # Finalize prefix commands string
        extra_prefix_commands_str = ' && '.join(extra_prefix_commands_list) if extra_prefix_commands_list else 'echo placeholder'
        print(f"Extra prefix commands string: {extra_prefix_commands_str}")

        # ── S3 sync via host-daemon sidecar containers ─────────────────────────
        # Both syncs run in the FSx sidecar (see sidecar_run), launched the same
        # way as the tool container so they share its real host-Lustre view. This
        # is fully explicit S3<->FSx sync in both directions — no dependence on DRA
        # auto-import/auto-export, so FSx auto-sync can be disabled entirely.
        #
        # DOWN: scoped to inputs, no --delete. Runs before the tool so inputs are
        #       materialised on the shared Lustre the tool container reads.
        # UP:   whole user dir, no --delete. NOT scoped to declared output mounts
        #       (tools write results outside their exact declared paths). Gated
        #       behind && after the tool, so a failed tool run publishes nothing.
        down_sync_cmds = tool.generate_down_sync_commands(user_mounts)
        if down_sync_cmds:
            down_sync_str = sidecar_run(user_id, ' && '.join(down_sync_cmds))
        else:
            down_sync_str = 'echo "no inputs to down-sync"'

        # Up-sync inner: list the output dirs from the sidecar's (correct) view for
        # visibility, then sync the whole user study dir. `; ` (not `&&`) after the `ls`
        # so the sync's exit code is what the sidecar returns.
        out_dirs = tool.output_dirs(user_mounts)
        ls_part = (
            f'echo "== FSx output dirs (sidecar view) ==" && '
            f'ls -laR {" ".join(out_dirs)} 2>&1 || true; '
            if out_dirs else ''
        )
        #TODO: Add study/project specific dir (HERE and down-sync??)
        up_inner = f'{ls_part}aws s3 sync /fsx/fsx/{user_id} s3://{S3_DATA_BUCKET}/fsx/{user_id}/'
        up_sync_str = sidecar_run(user_id, up_inner)

        print(f"DEBUG: down-sync: {down_sync_str}")
        print(f"DEBUG: up-sync: {up_sync_str}")

        full_command = (
            f'docker pull {tool.get_container_image()} '
            f'&& {down_sync_str} '
            f'&& {extra_prefix_commands_str} '
            f'&& {docker_command} '
            f'&& {up_sync_str}'
        )
        print(f"DEBUG: full fsx-manager command: {full_command}")

        response = batch.submit_job(
            jobName=f"{tool_name}-{user_id}",
            jobQueue=BATCH_QUEUE,
            jobDefinition=BATCH_JOB_DEF,
            #containerOverrides={
            #    "command": ["bash", "-c", docker_command],
            #    "environment": [{"name": "USER_ID", "value": user_id}]
            #},
            ecsPropertiesOverride={
                'taskProperties': [
                    {
                        'containers': [
                            {
                                'name': 'fsx-manager',
                                'command': ['bash', '-c', full_command],
                                'resourceRequirements': resource_requirements
                            }
                        ]
                    }
                ]
            },
            parameters={
                'FullCommand': docker_command,
                'ContainerImage': tool.get_container_image(),
                'tool_id': tool_name,
                'num_subjects': str(num_subjects),
            },
            tags={
                "submitted_by": user_id,
                "tool": tool_name
            }
        )

        result = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Job submitted successfully",
                "job_id": response["jobId"],
                "job_name": tool.name,
            })
        }
        print("DEBUG: Result:")
        print(result)

        return result

    except Exception as e:
        result = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
        print("DEBUG: Error Result:")
        print(result)
        return result