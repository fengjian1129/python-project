# -*- coding: utf-8 -*-
import sys
import pygame
from player import Player
def listen():
    # 监控键盘和鼠标事件
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            sys.exit()
def update_screen(screen, Player , t):
    # 每次循环时都重绘屏幕
    screen.fill(t.bg_color)
    Player.blitme()
    # 让最近绘制的屏幕可见
    pygame.display.flip()