import re

text = """
Politicians play a crucial role in everyone’s life. Their main task is to protect the country and the world from disasters and wars and to maintain a high standard of living. In my opinion, politicians should not focus their attention on solving environmental problems.

The main cause of global environmental problems is people’s use of plastic, single-use cups, and other materials that are harmful to nature. If people want to solve these problems, they would have to give up many of the conveniences that humanity has developed over a long period of time. However, politicians cannot solve this problem simply by banning certain products. For example, during Prohibition, the government banned alcohol, but people did not stop drinking; instead, they began to buy and sell it illegally. Similarly, if governments banned all single-use items, people would still find ways to obtain them.

On the other hand, individuals’ choices have a powerful and direct impact on the environment because they are one of the main causes of environmental damage. When millions of people decide to change their lifestyle and daily habits, corporations have to adapt to these changes. If society decides to reduce its use of disposable items, major companies will stop producing and selling them. Therefore, politicians are not the key to solving environmental problems; the impact of individuals’ choices is far more important.

To sum up, I disagree with the statement that politicians should spend their time and attention on solving global environmental problems rather than on making their country a safer and better place.
"""

# Извлекаем слова, корректно учитывая апострофы внутри слов (people's, don't)
words = re.findall(r"\b[a-zA-Z0-9]+(?:'[a-zA-Z0-9]+)?\b", text)

print(f"Общее количество слов: {len(words)}")
