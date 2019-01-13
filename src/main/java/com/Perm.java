package com;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

public class Perm {

  public static void main(String[] args) {
    Perm.perm(new int[]{1, 2, 1});
  }

  public static void perm(int[] nums) {
    Arrays.sort(nums);
    perm(nums, new LinkedList<>(), new boolean[nums.length]);
  }

  public static void perm(int[] nums, Deque<Integer> current, boolean[] flag) {
    if (current.size() == nums.length) {
      System.out.println(Arrays.toString(current.toArray()));
      return;
    }

    Integer last = null;
    for (int i = 0; i < nums.length; ++i) {
      if (flag[i]) {
        continue;
      }
      int n = nums[i];
      if (Objects.equals(n, last)) {
        continue;
      }
      last = n;
      flag[i] = true;
      current.add(n);
      perm(nums, current, flag);
      //
      flag[i] = false;
      current.removeLast();
    }
  }
}
