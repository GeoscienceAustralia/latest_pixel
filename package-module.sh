#!/usr/bin/env bash

echo $PATH

set -eu

umask 002

variant=prod
export module_dir=/g/data/v10/public/modules
#export agdc_module=agdc-py2-prod
#export agdc_module="agdc-py3/1.1.7"
export agdc_module="agdc-py3-prod/1.1.8"
export module_description="Datacube pixel rollover application"

while [[ $# > 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo Usage: $0 --moduledir ${module_dir} --agdc ${agdc_module}
        exit 0
        ;;
    --agdc)
        export agdc_module="$2"
        shift # past argument
        ;;
    --moduledir)
        export module_dir="$2"
        shift # past argument
        ;;
    *)
     # unknown option
    ;;
    esac
shift # past argument or value
done

module load ${agdc_module}

export module_name=agdc-pixel
# export version=`git describe --tags --always`
export version='1.0.0'
python_version=`python -c 'import sys; print ("%s.%s"%sys.version_info[:2])'`

export module_dest=${module_dir}/${module_name}/${version}
export python_dest=${module_dest}/lib/python${python_version}/site-packages

echo '# Packaging '$module_name' v '$version' to '$module_dest' #'
read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line

function render {
    vars='$module_dir:$agdc_module:$module_description:$module_name:$version:$module_dest:$python_dest'
    envsubst $vars < "$1" > "$2"
    echo Wrote "$2"
}

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 0
fi

# Setuptools requires the destination to be on the path, as it tests that the target is loadable.
echo "Creating directory"
mkdir -v -p "${python_dest}"

# Make the module dir as well?
mkdir -v -p "${module_dest}"

export PYTHONPATH="${PYTHONPATH}:${python_dest}"
echo $PYTHONPATH
echo "Installing:"
pip install . --prefix "${module_dest}" --no-deps
#python setup.py install --prefix "${module_dest}"

# Copy the scripts into the module dir
cp -v -r scripts "${module_dest}/"
render scripts/distributed.sh "${module_dest}/scripts/distributed.sh"
chmod a+x ${module_dest}/scripts/*


# make the environment.sh for the distributed worker to load
echo module use "${module_dir}/modulefiles" > "${module_dest}/scripts/environment.sh"
echo module load "${module_name}/${version}" >> "${module_dest}/scripts/environment.sh"

modulefile_dir="${module_dir}/modulefiles/${module_name}"
mkdir -v -p "${modulefile_dir}"

render modulefile.template "${modulefile_dir}/${version}"
