USE [dqthon]
GO

CREATE OR REPLACE TABLE [dbo].[participants](
	[participant_id] [varchar](max) NULL,
	[first_name] [varchar](max) NULL,
	[last_name] [varchar](max) NULL,
	[birth_date] [datetime] NULL,
	[address] [varchar](max) NULL,
	[phone_number] [varchar](max) NULL,
	[country] [varchar](max) NULL,
	[institute] [varchar](max) NULL,
	[occupation] [varchar](max) NULL,
	[register_time] [float] NULL,
	[postal_code] [varchar](max) NULL,
	[city] [varchar](max) NULL,
	[github_profile] [varchar](max) NULL,
	[cleaned_phone_number] [varchar](max) NULL,
	[team_name] [varchar](max) NULL,
	[email] [varchar](max) NULL,
	[register_at] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
