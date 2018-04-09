# -*- coding:utf-8 -*-

__author__ = "lsy"

import orm,sys
import asyncio
from models import User, Blog, Comment


async def test(loop):
    await orm.create_pool(user = "root", password = "lsy1", db = "awesome", loop = loop)
    # u = User(name = "test", admin = False, email = "test3@example.com", passwd = "1234567890", image = "about:blank")
    # await u.save()
    rs = await User.find(pk = "001523199293221ad0df70dd56e46abb0f62ac66c861df3000")
    print(rs)
    await orm.destroy_pool()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()
    if loop.is_closed():
        sys.exit()