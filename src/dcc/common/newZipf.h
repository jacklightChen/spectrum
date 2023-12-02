#pragma once

#include <cmath>
#include <vector>
//#include <glog/logging.h>

namespace dcc {

class Zipf {
 private:
  bool hasInit = false;

  double _alpha;  // zipf分布系数 这里注意n^alpha = 20即为二八分布
  int _n;         // 有多少值
  double _c = 0;  // 归一化常数
  std::vector<double> sum_probs;  // 预计算的累积概率

 public:
  void init(int nKeys, double alpha) {
    hasInit = true;
    _n = nKeys;
    _alpha = alpha;
    for (auto i = 1; i <= _n; i++) {
      _c = _c + (1.0 / std::pow((double)i, alpha));  // 累加每个事件的概率
    }
    _c = 1.0 / _c;             // 归一化常数
    sum_probs.resize(_n + 1);  // 分配空间
    sum_probs[0] = 0;          // 第一个元素为0
    for (int i = 1; i <= _n; i++) {
      sum_probs[i] =
          sum_probs[i - 1] + _c / std::pow((double)i, _alpha);  // 计算累积概率
    }
  }

  static Zipf &globalZipf() {
    static Zipf z;
    return z;
  }

  int value(double z) {
    //    CHECK(hasInit);
    int zipf_value = 0;  // Zipf分布的随机数
    int low, high, mid;  // 二分查找的边界

    // 根据累积概率映射到Zipf分布，使用二分查找加速
    low = 1, high = _n;
    do {
      mid = std::floor((low + high) / 2);                   // 取中间值
      if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {  // 找到对应的区间
        zipf_value = mid;  // 返回对应的排名
        break;
      } else if (sum_probs[mid] >= z) {  // 在左半边
        high = mid - 1;
      } else {  // 在右半边
        low = mid + 1;
      }
    } while (low <= high);

    //    DCHECK(v >= 0 && v < n_);
    return (zipf_value);
  }
};

}