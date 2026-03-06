#!/usr/bin/env bash
set -ex

# do_cmake.sh - 用于配置 Ceph 构建环境的脚本
# 功能：
# 1. 初始化 git 子模块
# 2. 根据操作系统和版本设置 Python 版本
# 3. 启用编译缓存（sccache 或 ccache）
# 4. 选择合适的 C/C++ 编译器
# 5. 创建构建目录并运行 CMake
# 6. 生成最小配置文件 ceph.conf
# 7. 输出关于构建类型的警告

# 初始化 git 子模块（如果存在 .git 目录）
if [ -d .git ]; then
    git submodule update --init --recursive --recommend-shallow
fi

# 设置默认构建目录和 Ceph git 目录
# 构建目录默认值为 'build'，可以通过设置 BUILD_DIR 环境变量来改变
# Ceph git 目录默认值为 '..'，可以通过设置 CEPH_GIT_DIR 环境变量来改变
: ${BUILD_DIR:=build}
: ${CEPH_GIT_DIR:=..}

# 检查构建目录是否已存在
if [ -e $BUILD_DIR ]; then
    echo "'$BUILD_DIR' dir already exists; either rm -rf '$BUILD_DIR' and re-run, or set BUILD_DIR env var to a different directory name"
    exit 1
fi

# 设置默认 Python 版本
PYBUILD="3"
# 添加 Ninja 构建系统参数
ARGS="${ARGS} -GNinja"

# 根据操作系统和版本设置 Python 版本
if [ -r /etc/os-release ]; then
  source /etc/os-release
  case "$ID" in
      fedora)
          # 根据 Fedora 版本设置 Python 版本
          if [ "$VERSION_ID" -ge "43" ] ; then
            PYBUILD="3.14"
          elif [ "$VERSION_ID" -ge "41" ] ; then
            PYBUILD="3.13"
          elif [ "$VERSION_ID" -ge "39" ] ; then
            PYBUILD="3.12"
          else
            # Fedora 37 and above
            PYBUILD="3.11"
          fi
          ;;
      almalinux|rocky|rhel|centos)
          # 根据 RHEL 系版本设置 Python 版本
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "10" ] ; then
              PYBUILD="3.12"
          elif [ "$MAJOR_VER" -ge "9" ] ; then
              PYBUILD="3.9"
          elif [ "$MAJOR_VER" -ge "8" ] ; then
              PYBUILD="3.6"
          fi
          ;;
      opensuse*|suse|sles)
          # openSUSE/SLES 使用默认 Python 版本
          PYBUILD="3"
          # 禁用 AMQP 和 Kafka 端点
          ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
          ARGS+=" -DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
          ;;
      ubuntu)
          # 根据 Ubuntu 版本设置 Python 版本
          MAJOR_VER=$(echo "$VERSION_ID" | sed -e 's/\..*$//')
          if [ "$MAJOR_VER" -ge "24" ] ; then
              PYBUILD="3.12"
          elif [ "$MAJOR_VER" -ge "22" ] ; then
              PYBUILD="3.10"
          fi
          ;;

  esac
elif [ "$(uname)" == FreeBSD ] ; then
  # FreeBSD 使用默认 Python 版本
  PYBUILD="3"
  # 禁用 AMQP 和 Kafka 端点
  ARGS+=" -DWITH_RADOSGW_AMQP_ENDPOINT=OFF"
  ARGS+=" -DWITH_RADOSGW_KAFKA_ENDPOINT=OFF"
else
  echo Unknown release
  exit 1
fi

# 添加 Python 版本参数
ARGS+=" -DWITH_PYTHON3=${PYBUILD}"

# 启用编译缓存（优先使用 sccache，其次使用 ccache）
if type sccache > /dev/null 2>&1 ; then
    echo "enabling sccache"
    ARGS+=" -DWITH_SCCACHE=ON"
elif type ccache > /dev/null 2>&1 ; then
    echo "enabling ccache"
    ARGS+=" -DWITH_CCACHE=ON"
fi

# 选择合适的 C/C++ 编译器（从 gcc-20 到 gcc-11 尝试）
cxx_compiler="g++"
c_compiler="gcc"
# 20 is used for more future-proof
for i in $(seq 20 -1 11); do
  if type -t gcc-$i > /dev/null; then
    cxx_compiler="g++-$i"
    c_compiler="gcc-$i"
    break
  fi
done
# 添加编译器参数
ARGS+=" -DCMAKE_CXX_COMPILER=$cxx_compiler"
ARGS+=" -DCMAKE_C_COMPILER=$c_compiler"

# 创建构建目录并进入
mkdir $BUILD_DIR
cd $BUILD_DIR

# 选择合适的 CMake 版本
# 优先级：cmake 4.x+ -> cmake3 -> cmake（默认）
if [ -z "${CMAKE}" ]; then
  if type cmake > /dev/null 2>&1 && cmake --version | grep -qE 'cmake version [4-9]\.'; then
      CMAKE=cmake
  elif type cmake3 > /dev/null 2>&1; then
      CMAKE=cmake3
  else
      CMAKE=cmake
  fi
fi

# 运行 CMake 配置
${CMAKE} $ARGS "$@" $CEPH_GIT_DIR || exit 1
set +x

# 生成最小配置文件 ceph.conf（用于查找插件）
cat <<EOF > ceph.conf
[global]
plugin dir = lib
erasure code dir = lib
EOF

echo done.

# 输出关于构建类型的警告
if [[ ! "$ARGS $@" =~ "-DCMAKE_BUILD_TYPE" ]]; then
    if [ -d ../.git ]; then
        printf "
****
WARNING: do_cmake.sh now creates debug builds by default if .git exists.
Performance may be severely affected. Please use -DCMAKE_BUILD_TYPE=RelWithDebInfo
if a performance sensitive build is required.
****
"
    else
        printf "
****
WARNING: do_cmake.sh now creates RelWithDebInfo builds by default when .git is absent.
****
"
    fi
fi

