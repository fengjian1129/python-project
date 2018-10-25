# -*- coding: utf-8 -*-
import sys
import pygame
from settings import Setting 
from game_function import *
def run_game():

    # 初始化游戏并创建一个屏幕对象
    pygame.init()

    # 设置屏幕大小
    t = Setting()
    # screen = pygame.display.set_mode((1200, 800))
    screen = pygame.display.set_mode((t.screen_width, t.screen_height))
    # 设置对话框的标题
    pygame.display.set_caption("Allen Iverson")
    # 设置背景颜色
    bg_color = t.bg_color

    # 创建一个player
    play = Player(screen)

    # 开始游戏的主循环
    while True:
        listen()

    update_screen(screen, play, t)
if __name__ == '__main__':
    run_game()