# -*- coding: utf-8 -*-
import sys
import pygame

class Setting():
    """存储游戏所有设置"""
    def __init__(self):
        # 屏幕设置
        self.screen_width = 1200
        self.screen_height = 500
        self.bg_color = (230, 230, 230)